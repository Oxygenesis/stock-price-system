package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

// Tick is the incoming price tick.
type Tick struct {
	Ticker    string    `json:"ticker"`
	Price     float64   `json:"price"`
	Timestamp time.Time `json:"timestamp"`
}

// OHLCV is aggregated bucketed data.
type OHLCV struct {
	Ticker      string    `json:"ticker"`
	BucketStart time.Time `json:"bucket_start"`
	Open        float64   `json:"open"`
	High        float64   `json:"high"`
	Low         float64   `json:"low"`
	Close       float64   `json:"close"`
	Count       int       `json:"count"`
}

const bucketDuration = 5 * time.Minute

func getKafkaBrokers() []string {
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		log.Fatal("KAFKA_BROKERS not set")
	}
	brokers := strings.Split(b, ",")
	log.Printf("Aggregator using Kafka brokers: %v", brokers)
	return brokers
}

func parseTick(raw string) (*Tick, error) {
	parts := strings.Split(raw, "|")
	var ticker, priceStr, tsStr string
	for _, p := range parts {
		if strings.HasPrefix(p, "TICKER=") {
			ticker = strings.TrimPrefix(p, "TICKER=")
		}
		if strings.HasPrefix(p, "PRICE=") {
			priceStr = strings.TrimPrefix(p, "PRICE=")
		}
		if strings.HasPrefix(p, "TS=") {
			tsStr = strings.TrimPrefix(p, "TS=")
		}
	}
	if ticker == "" || priceStr == "" || tsStr == "" {
		return nil, fmt.Errorf("malformed tick: %s", raw)
	}
	price, err := parseFloat(priceStr)
	if err != nil {
		return nil, fmt.Errorf("price parse error: %w", err)
	}
	ts, err := time.Parse(time.RFC3339, tsStr)
	if err != nil {
		return nil, fmt.Errorf("timestamp parse error: %w", err)
	}
	return &Tick{
		Ticker:    strings.ToUpper(ticker),
		Price:     price,
		Timestamp: ts,
	}, nil
}

func parseFloat(s string) (float64, error) {
	var v float64
	n, err := fmt.Sscanf(s, "%f", &v)
	if err != nil {
		return 0, err
	}
	if n != 1 {
		return 0, fmt.Errorf("expected one float, got %d", n)
	}
	return v, nil
}

func main() {
	ctx := context.Background()
	brokers := getKafkaBrokers()

	// Writers
	ticksWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    "ticks",
		Balancer: &kafka.Hash{}, // key-based partitioning by ticker
	})
	defer ticksWriter.Close()

	ohlcvWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    "ohlcv",
		Balancer: &kafka.Hash{},
	})
	defer ohlcvWriter.Close()

	feedURL := os.Getenv("FEED_URL")
	if feedURL == "" {
		log.Fatal("FEED_URL not set")
	}

	// Connect websocket
	log.Printf("Connecting to price feed generator at %s...", feedURL)
	conn, _, err := websocket.DefaultDialer.Dial(feedURL, nil)
	if err != nil {
		log.Fatalf("failed to connect to feed: %v", err)
	}
	defer conn.Close()
	log.Println("Connected to price feed generator")

	// Aggregation state
	type bucketKey struct {
		Ticker string
		Start  time.Time
	}
	var mu sync.Mutex
	ohlcvMap := make(map[bucketKey]*OHLCV)

	// Ticker to flush completed buckets periodically
	flushTicker := time.NewTicker(5 * time.Second)
	defer flushTicker.Stop()

	// Reader loop
	go func() {
		for {
			_, msgBytes, err := conn.ReadMessage()
			if err != nil {
				log.Printf("read websocket error: %v", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			raw := string(msgBytes)
			tick, err := parseTick(raw)
			if err != nil {
				log.Printf("parseTick error: %v raw=%s", err, raw)
				continue
			}
			log.Printf("tick received: ticker=%s price=%.2f ts=%s", tick.Ticker, tick.Price, tick.Timestamp.UTC().Format(time.RFC3339))

			// Publish raw tick to Kafka
			tickMsgVal, _ := json.Marshal(tick)
			err = ticksWriter.WriteMessages(ctx, kafka.Message{
				Key:   []byte(tick.Ticker),
				Value: tickMsgVal,
			})
			if err != nil {
				log.Printf("failed to write tick to kafka: %v", err)
			} else {
				log.Printf("published tick to kafka: ticker=%s", tick.Ticker)
			}

			// Aggregate into bucket
			bucketStart := tick.Timestamp.UTC().Truncate(bucketDuration)
			key := bucketKey{Ticker: tick.Ticker, Start: bucketStart}

			mu.Lock()
			bucket, exists := ohlcvMap[key]
			if !exists {
				bucket = &OHLCV{
					Ticker:      tick.Ticker,
					BucketStart: bucketStart,
					Open:        tick.Price,
					High:        tick.Price,
					Low:         tick.Price,
					Close:       tick.Price,
					Count:       1,
				}
				ohlcvMap[key] = bucket
			} else {
				if tick.Price > bucket.High {
					bucket.High = tick.Price
				}
				if tick.Price < bucket.Low {
					bucket.Low = tick.Price
				}
				bucket.Close = tick.Price
				bucket.Count++
			}
			mu.Unlock()
		}
	}()

	// Flush loop
	for range flushTicker.C {
		now := time.Now().UTC()
		toFlush := make([]*OHLCV, 0)
		mu.Lock()
		for key, bucket := range ohlcvMap {
			// flush buckets that are complete (older than current bucket window)
			if now.Sub(bucket.BucketStart) >= bucketDuration {
				toFlush = append(toFlush, bucket)
				delete(ohlcvMap, key)
			}
		}
		mu.Unlock()

		for _, o := range toFlush {
			data, _ := json.Marshal(o)
			err := ohlcvWriter.WriteMessages(ctx, kafka.Message{
				Key:   []byte(o.Ticker),
				Value: data,
			})
			if err != nil {
				log.Printf("failed to publish OHLCV for %s at %s: %v", o.Ticker, o.BucketStart, err)
			} else {
				log.Printf("published OHLCV: ticker=%s bucket=%s open=%.2f high=%.2f low=%.2f close=%.2f count=%d",
					o.Ticker,
					o.BucketStart.Format(time.RFC3339),
					o.Open, o.High, o.Low, o.Close, o.Count)
			}
		}
	}
}
