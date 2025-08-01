package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric/global"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// PriceState holds current price of a ticker.
type PriceState struct {
	Price float64
}

type Tick struct {
	Ticker    string
	Price     float64
	Timestamp time.Time
}

func randomTicker() string {
	letters := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	s := make([]rune, 4)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func initTracing(serviceName string, otlpEndpoint string) (*sdktrace.TracerProvider, error) {
	ctx := context.Background()
	client, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpEndpointTrim(otlpEndpoint)),
		otlptracegrpc.WithInsecure(), // assuming no TLS in local
	)
	if err != nil {
		return nil, err
	}
	res, _ := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(client),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp, nil
}

func initMeter(serviceName string) (*sdkmetric.MeterProvider, error) {
	res, _ := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceNameKey.String(serviceName)),
	)
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(sdkmetric.NewInexpensiveOTLPExporter())), // fallback if collector not configured
		sdkmetric.WithResource(res),
	)
	global.SetMeterProvider(provider)
	return provider, nil
}

// Helper: strip protocol if present
func otlpEndpointTrim(raw string) string {
	// expected form like http://otel-collector:4317 or otel-collector:4317
	// gRPC endpoint wants host:port without scheme in this client.
	if len(raw) >= 7 && (raw[:7] == "http://" || raw[:8] == "https://") {
		// strip scheme
		if raw[:7] == "http://" {
			return raw[7:]
		}
		return raw[8:]
	}
	return raw
}

func generateTick(state map[string]*PriceState) Tick {
	ticker := randomTicker()
	s, ok := state[ticker]
	if !ok {
		s = &PriceState{Price: 50 + rand.Float64()*450}
		state[ticker] = s
	}

	if rand.Float64() < 0.1 {
		if rand.Intn(2) == 0 {
			s.Price *= 1 + float64(rand.Intn(2))*0.05
		} else {
			s.Price /= 1 + float64(rand.Intn(2))*0.05
		}
	} else {
		change := float64(rand.Intn(10) + 1)
		if rand.Intn(2) == 0 {
			s.Price += change
		} else {
			s.Price -= change
		}
		if s.Price < 1 {
			s.Price = 1
		}
	}

	return Tick{
		Ticker:    ticker,
		Price:     s.Price,
		Timestamp: time.Now(),
	}
}

func feedHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	tr := otel.Tracer("generator")
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}
	defer func() {
		log.Println("WebSocket connection closed")
		conn.Close()
	}()

	log.Println("WebSocket client connected to feed")
	state := map[string]*PriceState{}
	var mu sync.RWMutex

	// metric: tick count per second
	tickCounter, _ := global.Meter("generator").Int64Counter("ticks.generated")
	batchTimer, _ := global.Meter("generator").Float64Histogram("batch.duration_ms")

	tickerTicker := time.NewTicker(20 * time.Millisecond) // 5 ticks per batch => ~250 ticks/sec, you can adjust to hit ~50/sec by reducing concurrency or frequency
	defer tickerTicker.Stop()

	for range tickerTicker.C {
		start := time.Now()
		_, span := tr.Start(ctx, "generate-batch")
		log.Println("Starting tick batch generation")
		for i := 0; i < 5; i++ {
			mu.Lock()
			tick := generateTick(state)
			mu.Unlock()

			msg := fmt.Sprintf("TICKER=%s|PRICE=%.2f|TS=%s", tick.Ticker, tick.Price, tick.Timestamp.UTC().Format(time.RFC3339))
			log.Printf("Generated tick: %s", msg)
			tickCounter.Add(ctx, 1, attribute.String("ticker", tick.Ticker))

			if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
				log.Printf("write error: %v", err)
			} else {
				log.Printf("Sent tick to websocket client: %s", tick.Ticker)
			}
		}
		span.End()
		duration := time.Since(start).Seconds() * 1000.0
		batchTimer.Record(ctx, duration)
		log.Printf("Finished batch in %.2fms", duration)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Tracing setup with endpoint from env override, fallback to default collector address.
	otelEndpoint := "otel-collector:4317"
	tp, err := initTracing("generator", otelEndpoint)
	if err != nil {
		log.Printf("tracer init failure: %v", err)
	} else {
		defer tp.Shutdown(context.Background())
	}

	_, _ = initMeter("generator") // ignore error for brevity

	http.HandleFunc("/feed", feedHandler)
	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		fmt.Fprint(w, "ok")
	})
	log.Println("Generator WS listening on :8000/feed")
	if err := http.ListenAndServe(":8000", nil); err != nil {
		log.Fatalf("http serve failed: %v", err)
	}
}
