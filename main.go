package main

import (
	"context"
	"log"
	"os"

	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
)

var (
	redisClient *redis.Client
	ctx         = context.Background()
)

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
