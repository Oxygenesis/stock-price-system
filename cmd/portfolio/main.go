package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"io/ioutil"

	"github.com/gorilla/mux"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

type PriceDetail struct {
	Ticker    string `json:"ticker"`
	LastPrice string `json:"last_price"`
	UpdatedAt string `json:"updated_at"`
}

type PortfolioEntry struct {
	Ticker string  `json:"ticker"`
	Shares float64 `json:"shares"`
}

type PortfolioView struct {
	Entries []struct {
		Ticker    string  `json:"ticker"`
		Shares    float64 `json:"shares"`
		LastPrice float64 `json:"last_price"`
		Value     float64 `json:"value"`
	} `json:"entries"`
	TotalValue float64 `json:"total_value"`
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

func fetchPriceDetail(url string) (*PriceDetail, error) {
	var pd PriceDetail
	const attempts = 3
	for i := 1; i <= attempts; i++ {
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("attempt %d: error fetching %s: %v", i, url, err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if resp.StatusCode != 200 {
			log.Printf("attempt %d: non-200 fetching %s: %d", i, url, resp.StatusCode)
			resp.Body.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err := json.Unmarshal(body, &pd); err != nil {
			log.Printf("attempt %d: unmarshal error for %s: %v", i, url, err)
			continue
		}
		return &pd, nil
	}
	return nil, fmt.Errorf("failed to fetch price detail from %s", url)
}

func main() {
	ctx := context.Background()
	tp, err := initTracer("portfolio")
	if err != nil {
		log.Printf("tracer init error: %v", err)
	}
	if tp != nil {
		defer tp.Shutdown(ctx)
	}
	tr := otel.Tracer("portfolio")

	stockDetailURL := os.Getenv("STOCK_DETAIL_URL")
	if stockDetailURL == "" {
		log.Fatal("STOCK_DETAIL_URL not set")
	}

	portfolio := []PortfolioEntry{
		{"ABCD", 10},
		{"WXYZ", 5},
		{"CCDA", 5},
		{"PANI", 5},
		{"CDIA", 5},
	}

	r := mux.NewRouter()
	r.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		fmt.Fprint(w, "ok")
	})
	r.HandleFunc("/portfolio", func(w http.ResponseWriter, req *http.Request) {
		ctx2, span := tr.Start(req.Context(), "compute-portfolio")
		defer span.End()

		var view PortfolioView
		for _, entry := range portfolio {
			span.AddEvent("fetching price", oteltrace.WithAttributes(attribute.String("ticker", entry.Ticker)))
			url := fmt.Sprintf("%s/price/%s", stockDetailURL, entry.Ticker)
			pd, err := fetchPriceDetail(url)
			if err != nil {
				log.Printf("skipping %s: %v", entry.Ticker, err)
				continue
			}
			price := 0.0
			fmt.Sscanf(pd.LastPrice, "%f", &price)
			value := price * entry.Shares
			view.Entries = append(view.Entries, struct {
				Ticker    string  `json:"ticker"`
				Shares    float64 `json:"shares"`
				LastPrice float64 `json:"last_price"`
				Value     float64 `json:"value"`
			}{entry.Ticker, entry.Shares, price, value})
			view.TotalValue += value
		}
		if len(view.Entries) == 0 {
			http.Error(w, "no data available", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(view)
	})

	log.Println("Portfolio listening on :8082")
	log.Fatal(http.ListenAndServe(":8082", r))
}
