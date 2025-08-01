package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

func initTracer(serviceName string) (*sdktrace.TracerProvider, error) {
	ctx := context.Background()
	client, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	res, _ := sdkresource.Merge(
		sdkresource.Default(),
		sdkresource.NewWithAttributes(
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

func getKafkaBrokers() []string {
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		log.Fatal("KAFKA_BROKERS not set")
	}
	return strings.Split(b, ",")
}

func main() {
	ctx := context.Background()
	tp, err := initTracer("stock-detail")
	if err != nil {
		log.Printf("tracer init error: %v", err)
	}
	if tp != nil {
		defer tp.Shutdown(ctx)
	}
	tr := otel.Tracer("stock-detail")

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		log.Fatal("REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}
	log.Printf("Connected to Redis at %s", redisAddr)

	brokers := getKafkaBrokers()
	log.Printf("Stock-detail using Kafka brokers: %v", brokers)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    "ticks",
		GroupID:  "stock-detail-group",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	r := mux.NewRouter()
	r.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		fmt.Fprint(w, "ok")
	})
	r.HandleFunc("/price/{ticker}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		ticker := strings.ToUpper(vars["ticker"])
		if ticker == "" {
			http.Error(w, "missing ticker", http.StatusBadRequest)
			return
		}
		ctx2, span := tr.Start(req.Context(), "price-lookup",
			oteltrace.WithAttributes(attribute.String("ticker", ticker)))
		defer span.End()

		data, err := rdb.HGetAll(ctx2, "stock:"+ticker).Result()
		if err != nil {
			log.Printf("redis HGetAll error for %s: %v", ticker, err)
			http.Error(w, "redis error", http.StatusInternalServerError)
			return
		}
		if len(data) == 0 {
			log.Printf("cache miss for ticker %s", ticker)
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		resp := map[string]string{
			"ticker":     ticker,
			"last_price": data["last_price"],
			"updated_at": data["updated_at"],
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	go func() {
		log.Println("Stock-detail HTTP on :8080")
		if err := http.ListenAndServe(":8080", r); err != nil {
			log.Fatalf("http serve: %v", err)
		}
	}()

	log.Println("Stock-detail started, consuming ticks...")
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("kafka read error: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		ctx2, span := tr.Start(ctx, "process-tick")
		val := string(m.Value)
		ticker := ""
		priceStr := ""
		parts := strings.Split(val, "|")
		for _, p := range parts {
			if strings.HasPrefix(p, "TICKER=") {
				ticker = strings.TrimPrefix(p, "TICKER=")
			}
			if strings.HasPrefix(p, "PRICE=") {
				priceStr = strings.TrimPrefix(p, "PRICE=")
			}
		}
		if ticker == "" || priceStr == "" {
			log.Printf("malformed tick, skipping: %s", val)
			span.End()
			continue
		}
		if err := rdb.HSet(ctx2, "stock:"+strings.ToUpper(ticker),
			"last_price", priceStr,
			"updated_at", time.Now().UTC().Format(time.RFC3339)).Err(); err != nil {
			log.Printf("redis HSet error: %v", err)
		} else {
			log.Printf("updated redis for %s price=%s", strings.ToUpper(ticker), priceStr)
		}
		span.SetAttributes(attribute.String("ticker", ticker), attribute.String("price", priceStr))
		span.End()
	}
}
