package main

import (
	"context"
	"log"
	"math"
	"os"

	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
)

const (
	PP_DEFAULT_ID         = 1
	PP_FALLBACK_ID        = 2
	PAYMENT_REDIS         = "payment"
	PENDING_PAYMENT_REDIS = "pending_payment"
	WORKERS               = 16
	RFC3339Milli          = "2006-01-02T15:04:05.000Z07:00"
)

var (
	redisClient  *redis.Client
	ctx          = context.Background()
	paymentQueue chan CreatePaymentRequest
)

type CreatePaymentRequest struct {
	CorrelationID string  `json:"correlation_id"`
	Amount        float64 `json:"amount"`
}

type SummaryRequest struct {
	From *string `query:"from"`
	To   *string `query:"to"`
}

type SummaryResponse struct {
	Default  PaymentSummary `json:"default"`
	Fallback PaymentSummary `json:"fallback"`
}

type PaymentSummary struct {
	Request int     `json:"total"`
	Amount  float64 `json:"amount"`
}

type PaymentRequest struct {
	CorrelationID    string  `json:"correlationID"`
	RequestedAt      string  `json:"requestedAt"`
	Amount           float64 `json:"amount"`
	PaymentProcessor *int    `json:"paymentProcessor"`
}

type HealthCheck struct {
	MinResponseTiming int  `json:"minResponseTiming"`
	Failing           bool `json:"failing"`
}

func Redis() (*redis.Client, error) {
	addr := os.Getenv("REDIS_ADDR")
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   0,
	})
	err := rdb.Ping(ctx).Err()
	if err != nil {
		return nil, err
	}

	log.Println("connected to redis :)", addr)
	return rdb, nil
}

func Decimals(n float64) float64 {
	multiply := n * 100
	epsilon := math.Copysign(1e-7, multiply)
	return math.Round(multiply+epsilon) / 100
}

func Payment(p PaymentRequest, pID int) error {}

func main() {
	var err error
	redisClient, err = Redis()
	if err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}

	log.Println("starting HTTP server on port :8080 :)")
	if err := fasthttp.ListenAndServe(":8080", func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.WriteString("200\n")
	}); err != nil {
		log.Fatalf("error starting HTTP server: %s :()", err)
	}
}
