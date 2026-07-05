FROM golang:1.20-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go env -w GOPROXY=https://goproxy.cn,direct || true
RUN go mod download
COPY cmd ./cmd
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o bot-fleet cmd/main.go

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/bot-fleet .
EXPOSE 8081
CMD ["./bot-fleet"]
