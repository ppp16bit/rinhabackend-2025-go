package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
)

const (
	PP_DEFAULT_ID         = 1
	PP_FALLBACK_ID        = 2
	PAYMENT_REDIS         = "payment"
	PENDING_PAYMENT_REDIS = "pending_payment"
	BEST_PP_REDIS         = "best_pp"
	WORKERS               = 12
	RFC3339Milli          = "2006-01-02T15:04:05.000Z07:00"
)

var (
	redisClient  *redis.Client
	ctx          = context.Background()
	paymentQueue chan CreatePaymentRequest
)

type CreatePaymentRequest struct {
	CorrelationID string `json:"correlation_id"`
	Amount        int64  `json:"amount"`
}

type SummaryRequest struct {
	From *string `query:"from"`
	To   *string `query:"to"`
}

type SummaryResponse struct {
	Default  PaymentSummary `json:"default"`
	Fallback PaymentSummary `json:"fallback"`
}

type summaryResponseFloat struct {
	Default struct {
		Total  int     `json:"total_requests"`
		Amount float64 `json:"total_amount"`
	} `json:"default"`
	Fallback struct {
		Total  int     `json:"total_requests"`
		Amount float64 `json:"total_amount"`
	} `json:"fallback"`
}

type PaymentSummary struct {
	Request int   `json:"total_requests"`
	Amount  int64 `json:"total_amount"`
}

type PaymentRequest struct {
	CorrelationID    string `json:"correlation_id"`
	RequestedAt      string `json:"requested_at"`
	Amount           int64  `json:"amount"`
	PaymentProcessor *int   `json:"payment_processor"`
}

type HealthCheck struct {
	MinResponseTiming int  `json:"minResponseTiming"`
	Failing           bool `json:"failing"`
}

func Redis() (*redis.Client, error) {
	addr := os.Getenv("REDIS_ADDR")
	rdb := redis.NewClient(&redis.Options{
		Addr:            addr,
		DB:              0,
		PoolSize:        10,
		MinIdleConns:    2,
		ConnMaxLifetime: 5 * time.Minute,
		ReadTimeout:     3 * time.Second,
		WriteTimeout:    3 * time.Second,
		DialTimeout:     2 * time.Second,
	})
	err := rdb.Ping(ctx).Err()
	if err != nil {
		return nil, err
	}
	log.Println("connected to redis :)", addr)
	return rdb, nil
}

func GetProcessorURL(processorID int) string {
	if processorID == PP_DEFAULT_ID {
		return os.Getenv("PAYMENT_PROCESSOR_DEFAULT_URL")
	}
	return os.Getenv("PAYMENT_PROCESSOR_FALLBACK_URL")
}

func processorHealthCheck(processorID int) (*HealthCheck, error) {
	url := GetProcessorURL(processorID)

	response, err := http.Get(url + "/payments/service-health")
	if err != nil {
		fmt.Printf("HealthCheck: (error): Processor=[ %d ] - %v", processorID, err)
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		log.Printf("HealthCheck: Processor= [ %d ] Status= [ %d ]", processorID, response.StatusCode)
		return nil, fmt.Errorf("StatusCode= [ %d ]", response.StatusCode)
	}

	var health HealthCheck
	if err := json.NewDecoder(response.Body).Decode(&health); err != nil {
		return nil, fmt.Errorf("decode (error): [ %w ]", err)
	}

	log.Printf("HealthCheck: Processor= [ %d ] - Failing= [ %v ] - MinResponse= [ %d ]", processorID, health.Failing, health.MinResponseTiming)
	return &health, nil
}

func processorCreatePayment(p *CreatePaymentRequest, processorID int) (*PaymentRequest, error) {
	req := &PaymentRequest{
		Amount:        p.Amount,
		CorrelationID: p.CorrelationID,
		RequestedAt:   time.Now().UTC().Format(RFC3339Milli),
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshall: (error): %w", err)
	}

	url := GetProcessorURL(processorID)

	response, err := http.Post(url+"/payments", "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return nil, fmt.Errorf("request: (error): %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(response.Body)
		return nil, fmt.Errorf("processor= [ %d ] - statuscode= [ %d ]: %s", processorID, response.StatusCode, string(body))
	}
	log.Printf("Payment: sent to Processor= [ %d ] - CorrelationID= [ %s ]", processorID, p.CorrelationID)
	return req, nil
}

func Payment(p PaymentRequest, pID int) error {
	requestedAt, err := time.Parse(RFC3339Milli, p.RequestedAt)
	if err != nil {
		return fmt.Errorf("invalid time format: %w", err)
	}

	p.PaymentProcessor = &pID

	data, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("json marshall error: %w", err)
	}

	if err := redisClient.ZAdd(ctx, PAYMENT_REDIS, redis.Z{
		Score:  float64(requestedAt.UnixMilli()),
		Member: data,
	}).Err(); err != nil {
		return fmt.Errorf("zAdd error: %w", err)
	}
	log.Printf("Stored Payment [ CorrelationID=%s, ProcessorID=%d ]", p.CorrelationID, pID)
	return nil
}

func BestProcessor() *int {
	n, err := redisClient.Get(ctx, BEST_PP_REDIS).Result()
	if err != nil {
		log.Printf("Redis error using Fallback: %v", err)
		defaultID := PP_DEFAULT_ID
		return &defaultID
	}

	id, err := strconv.Atoi(n)
	if err != nil {
		log.Printf("Invalid processor using Fallback: %v", err)
		defaultID := PP_DEFAULT_ID
		return &defaultID
	}

	log.Printf("Best processor: %d", id)
	return &id
}

func processPayment(p CreatePaymentRequest) error {
	processorID := BestProcessor()
	if processorID == nil {
		return fmt.Errorf("no processor available")
	}

	request, err := processorCreatePayment(&p, *processorID)
	if err != nil {
		return fmt.Errorf("payment processor failed: %w", err)
	}

	if err := Payment(*request, *processorID); err != nil {
		return fmt.Errorf("failed to store payment: %w", err)
	}

	return nil
}

func workers(paymentQueue chan CreatePaymentRequest) {
	for i := 0; i < WORKERS; /*12*/ i++ {
		go func(worker int) {
			fmt.Printf("Worker: [ %d ] started\n", worker)
			for pReq := range paymentQueue {
				fmt.Printf("Worker: [ %d ] processing - CorrelationID= [ %s ]\n", worker, pReq.CorrelationID)
				if err := processPayment(pReq); err != nil {
					fmt.Printf("Worker: [ %d ] failed to process payment: %v. Re-enqueuing...\n", worker, err)
					go func() {
						time.Sleep(2 * time.Second)
						paymentQueue <- pReq
					}()
				}
			}
		}(i)
	}
}

func healthMonitoring() {
	instanceID := os.Getenv("INSTANCE_ID")
	var delay time.Duration

	switch instanceID {
	case "api1":
		delay = 0
	case "api2":
		delay = 2500 * time.Millisecond
	default:
		return
	}
	time.Sleep(delay)

	log.Println("--- Health Monitor ---")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		processorIDs := [2]int{PP_DEFAULT_ID, PP_FALLBACK_ID}
		pResponses := make(map[int]int)

		for _, processorID := range processorIDs {
			response, err := processorHealthCheck(processorID)
			if err != nil || response.Failing {
				pResponses[processorID] = math.MaxInt
				continue
			}
			pResponses[processorID] = response.MinResponseTiming
		}

		defaultTime := pResponses[PP_DEFAULT_ID]
		fallbackTime := pResponses[PP_FALLBACK_ID]
		var BestProcessor int
		var bestTime int

		if defaultTime != math.MaxInt && (fallbackTime == math.MaxInt || defaultTime <= fallbackTime) {
			BestProcessor = PP_DEFAULT_ID
			bestTime = defaultTime
		} else {
			BestProcessor = PP_FALLBACK_ID
			bestTime = fallbackTime
		}

		redisClient.Set(ctx, BEST_PP_REDIS, BestProcessor, 0)
		log.Printf("Redis: update Best Processor to [ %d ] - MinResponseTime= [ %dms ]", BestProcessor, bestTime)
	}
}

func summary(ctx *fasthttp.RequestCtx) {
	const maxResults = 1000

	from := string(ctx.QueryArgs().Peek("from"))
	to := string(ctx.QueryArgs().Peek("to"))

	parseTime := func(timeStr string) (*time.Time, error) {
		if timeStr == "" {
			return nil, nil
		}
		layouts := []string{
			time.RFC3339,
			RFC3339Milli,
			"2006-01-02T15:04:05",
			"2006-01-02",
		}

		for _, layout := range layouts {
			if parsedTime, parseErr := time.Parse(layout, timeStr); parseErr == nil {
				return &parsedTime, nil
			}
		}
		return nil, fmt.Errorf("invalid datetime format: [ %s ]", timeStr)
	}

	fromTime, err := parseTime(from)
	if err != nil {
		log.Printf("Summary: Invalid 'from' parameter: %v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBodyString(`{"error":"invalid 'from' datetime format"}`)
		return
	}

	toTime, err := parseTime(to)
	if err != nil {
		log.Printf("Summary: Invalid 'to' parameter: %v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBodyString(`{"error":"invalid 'to' datetime format"}`)
		return
	}

	if fromTime != nil && toTime != nil && fromTime.After(*toTime) {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBodyString(`{"error":"'from' date must be before 'to' date"}`)
		return
	}

	maxScore := "+inf"
	minScore := "-inf"

	if fromTime != nil {
		minScore = fmt.Sprintf("%d", fromTime.UnixMilli())
	}
	if toTime != nil {
		maxScore = fmt.Sprintf("%d", toTime.UnixMilli())
	}
	log.Printf("Summary: Querying range [ %s ] to [ %s ] ", minScore, maxScore)

	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	results, err := redisClient.ZRangeByScore(queryCtx, PAYMENT_REDIS, &redis.ZRangeBy{
		Min:    minScore,
		Max:    maxScore,
		Offset: 0,
		Count:  maxResults,
	}).Result()

	if err != nil {
		log.Printf("Summary: Redis query failed: %v", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString(`{"error":"failed to fetch payments"}`)
		return
	}

	response := SummaryResponse{
		Default:  PaymentSummary{Request: 0, Amount: 0},
		Fallback: PaymentSummary{Request: 0, Amount: 0},
	}

	var (
		processedCount   = 0
		skippedCount     = 0
		invalidCount     = 0
		unknownProcessor = 0
	)

	for _, result := range results {
		var payment PaymentRequest

		if err := json.Unmarshal([]byte(result), &payment); err != nil {
			log.Printf("Summary: Failed to unmarshal payment: %v", err)
			invalidCount++
			continue
		}

		if payment.PaymentProcessor == nil {
			log.Printf("Summary: Payment without processor ID: %s", payment.CorrelationID)
			skippedCount++
			continue
		}
		processedCount++

		switch *payment.PaymentProcessor {
		case PP_DEFAULT_ID:
			response.Default.Request++
			response.Default.Amount += payment.Amount
		case PP_FALLBACK_ID:
			response.Fallback.Request++
			response.Fallback.Amount += payment.Amount
		default:
			log.Printf("Summary: unknow processor ID [ %d ] for Payment: [ %s ]", *payment.PaymentProcessor, payment.CorrelationID)
			unknownProcessor++
		}
	}
	responseFloat := summaryResponseFloat{
		Default: struct {
			Total  int     `json:"total_requests"`
			Amount float64 `json:"total_amount"`
		}{
			Total:  response.Default.Request,
			Amount: float64(response.Default.Amount) / 100,
		},
		Fallback: struct {
			Total  int     `json:"total_requests"`
			Amount float64 `json:"total_amount"`
		}{
			Total:  response.Fallback.Request,
			Amount: float64(response.Fallback.Amount) / 100.0,
		},
	}

	log.Printf("Summary: Processed= [ %d ], Invalid= [ %d ], Skipped= [% d ], Unknown= [ %d ]", processedCount, invalidCount, skippedCount, unknownProcessor)

	if len(results) >= maxResults {
		log.Printf("Summary Warning = Result limit [ %d ] reached data may be incomplete", maxResults)
	}

	ctx.SetContentType("application/json")
	if err := json.NewEncoder(ctx).Encode(responseFloat); err != nil {
		log.Printf("Summary: Failed to encode response: %v", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString(`{"error":"failed to encode response"}`)
		return
	}
	log.Printf("Summary: Success - Default: [ %d ] payments [ %.2f ], Fallback: [ %d ] payments [ %.2f ]", responseFloat.Default.Total, responseFloat.Default.Amount, responseFloat.Fallback.Total, responseFloat.Fallback.Amount)
}

func purifiedPayment(ctx *fasthttp.RequestCtx) {
	log.Println("Purifying all payments in Redis")

	if err := redisClient.Del(ctx, PAYMENT_REDIS).Err(); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString(fmt.Sprintf(`{"error":"failed to purge payments: %v"}`, err))
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.SetBodyString(`{"status":"payments purged successfully"}`)
}

func createPayment(ctx *fasthttp.RequestCtx) {
	var paymentAux struct {
		CorrelationID string  `json:"correlationId"`
		Amount        float64 `json:"amount"`
	}

	if err := json.Unmarshal(ctx.PostBody(), &paymentAux); err != nil {
		log.Println("Invalid payment request JSON")
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBodyString(`{"error":"invalid JSON"}`)
		return
	}
	amountInCents := int64(math.Round(paymentAux.Amount * 100))

	if paymentAux.CorrelationID == "" || amountInCents <= 0 {
		log.Println("Missing or Invalid payment fields")
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBodyString(`{"error":"missing or invalid fields"}`)
		return
	}

	p := CreatePaymentRequest{
		CorrelationID: paymentAux.CorrelationID,
		Amount:        amountInCents,
	}

	select {
	case paymentQueue <- p:
		log.Printf("Payment Queue: Successfully!! - CorrelationID= [ %s ] - Amount= [ %d ]", p.CorrelationID, p.Amount)
		ctx.SetStatusCode(fasthttp.StatusNoContent)
	default:
		log.Println("Payment queue full: Rejecting request")
		ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
		ctx.SetBodyString(`{"error":"Payment queue is full: Try again later.."}`)
	}
}

func init() {
	debug.SetGCPercent(20)
	debug.SetMemoryLimit(25 << 20)

	if maxprocs := os.Getenv("GOMAXPROCS"); maxprocs == "" {
		runtime.GOMAXPROCS(1)
	}
}

func main() {
	var err error
	redisClient, err = Redis()
	if err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}

	go func() {
		exists, err := redisClient.Exists(ctx, BEST_PP_REDIS).Result()
		time.Sleep(1 * time.Second)
		if err != nil || exists == 0 {
			log.Println("Initializing best processor as Default")
			redisClient.Set(ctx, BEST_PP_REDIS, PP_DEFAULT_ID, 0)
		}
	}()

	paymentQueue = make(chan CreatePaymentRequest, 1000)
	go workers(paymentQueue)
	go healthMonitoring()

	router := func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/payments":
			if ctx.IsPost() {
				createPayment(ctx)
				return
			}
			ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		case "/payments/purify":
			if ctx.IsPost() {
				purifiedPayment(ctx)
				return
			}
			ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		case "/payments-summary":
			if ctx.IsGet() {
				summary(ctx)
				return
			}
			ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		default:
			ctx.SetStatusCode(fasthttp.StatusNotFound)
			ctx.SetBodyString(`{"error":"Not Found"}`)
		}
	}
	log.Println("Starting HTTP server on port: [ 8080 ] :)")
	if err := fasthttp.ListenAndServe(":8080", router); err != nil {
		log.Fatalf("error starting HTTP server: %s :(", err)
	}
}
