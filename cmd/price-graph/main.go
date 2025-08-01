package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// OHLCV represents the aggregated bucketed data.
type OHLCV struct {
	Ticker      string    `json:"ticker"`
	BucketStart time.Time `json:"bucket_start"`
	Open        float64   `json:"open"`
	High        float64   `json:"high"`
	Low         float64   `json:"low"`
	Close       float64   `json:"close"`
	Volume      float64   `json:"volume"`
}

func getKafkaBrokers() []string {
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		log.Fatal("KAFKA_BROKERS not set")
	}
	brokers := strings.Split(b, ",")
	log.Printf("Using Kafka brokers: %v", brokers)
	return brokers
}

func getDB() *sql.DB {
	user := os.Getenv("PG_USER")
	pass := os.Getenv("PG_PASSWORD")
	host := os.Getenv("PG_HOST")
	dbname := os.Getenv("PG_DB")
	if user == "" || pass == "" || host == "" || dbname == "" {
		log.Fatal("Postgres env vars missing")
	}
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, pass, host, dbname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	// ensure table
	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS ohlcv (
  ticker TEXT,
  bucket_start TIMESTAMPTZ,
  open DOUBLE PRECISION,
  high DOUBLE PRECISION,
  low DOUBLE PRECISION,
  close DOUBLE PRECISION,
  volume DOUBLE PRECISION,
  PRIMARY KEY (ticker, bucket_start)
);`)
	if err != nil {
		log.Fatalf("ensure table: %v", err)
	}
	return db
}

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

func main() {
	ctx := context.Background()

	tp, err := initTracer("price-graph")
	if err != nil {
		log.Printf("failed to init tracer: %v", err)
	} else {
		defer func() {
			_ = tp.Shutdown(ctx)
		}()
	}
	tr := otel.Tracer("price-graph")

	brokers := getKafkaBrokers()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    "ohlcv",
		GroupID:  "price-graph-group",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	db := getDB()
	defer db.Close()

	// HTTP API
	r := mux.NewRouter()
	r.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		fmt.Fprint(w, "ok")
	})
	r.HandleFunc("/ohlcv/{ticker}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		ticker := strings.ToUpper(vars["ticker"])
		if ticker == "" {
			http.Error(w, "ticker required", http.StatusBadRequest)
			return
		}
		qctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancel()

		rows, err := db.QueryContext(qctx, `
SELECT ticker, bucket_start, open, high, low, close, volume
FROM ohlcv
WHERE ticker = $1
ORDER BY bucket_start DESC
LIMIT 50`, ticker)
		if err != nil {
			http.Error(w, "db query error", http.StatusInternalServerError)
			log.Printf("db query error for %s: %v", ticker, err)
			return
		}
		defer rows.Close()

		var out []OHLCV
		for rows.Next() {
			var o OHLCV
			if err := rows.Scan(&o.Ticker, &o.BucketStart, &o.Open, &o.High, &o.Low, &o.Close, &o.Volume); err != nil {
				log.Printf("scan error: %v", err)
				continue
			}
			out = append(out, o)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})

	go func() {
		log.Println("Price-graph HTTP on :8081")
		if err := http.ListenAndServe(":8081", r); err != nil {
			log.Fatalf("http serve: %v", err)
		}
	}()

	log.Println("Price-graph consuming OHLCV and persisting to TimescaleDB")
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("read ohlcv error: %v", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		func() {
			msgCtx, span := tr.Start(ctx, "process-ohlcv", oteltrace.WithAttributes(
				attribute.String("raw", string(m.Value)),
			))
			defer span.End()

			var o OHLCV
			if err := json.Unmarshal(m.Value, &o); err != nil {
				log.Printf("unmarshal error: %v", err)
				span.RecordError(err)
				return
			}

			upperTicker := strings.ToUpper(o.Ticker)
			span.SetAttributes(attribute.String("ticker", upperTicker))

			upCtx, cancel := context.WithTimeout(msgCtx, 1*time.Second)
			defer cancel()

			res, err := db.ExecContext(upCtx, `
INSERT INTO ohlcv (ticker, bucket_start, open, high, low, close, volume)
VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (ticker, bucket_start) DO UPDATE SET
  high = GREATEST(ohlcv.high, EXCLUDED.high),
  low = LEAST(ohlcv.low, EXCLUDED.low),
  close = EXCLUDED.close,
  volume = EXCLUDED.volume
`, upperTicker, o.BucketStart, o.Open, o.High, o.Low, o.Close, o.Volume)
			if err != nil {
				log.Printf("db upsert error for %s: %v", upperTicker, err)
				span.RecordError(err)
				return
			}
			if rowsAffected, _ := res.RowsAffected(); rowsAffected > 0 {
				log.Printf("upserted OHLCV ticker=%s bucket=%s open=%.2f high=%.2f low=%.2f close=%.2f volume=%.2f",
					upperTicker, o.BucketStart.UTC().Format(time.RFC3339), o.Open, o.High, o.Low, o.Close, o.Volume)
			}
		}()
	}
}
