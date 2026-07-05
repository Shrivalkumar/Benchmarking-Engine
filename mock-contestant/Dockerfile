FROM golang:1.20-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o matching-engine main.go

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/matching-engine .
EXPOSE 8080
CMD ["./matching-engine"]
