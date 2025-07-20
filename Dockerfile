FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o main ./main.go

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /app/main .

EXPOSE 9999

CMD ["./main"]