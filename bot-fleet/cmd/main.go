package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// Request configuration
type StartRequest struct {
	BenchmarkRunID  string `json:"benchmark_run_id"`
	TargetURL       string `json:"target_url"`
	DurationSeconds int    `json:"duration_seconds"`
	TPS             int    `json:"tps"`
	Concurrency     int    `json:"concurrency"`
}

// Telemetry message structure sent to Kafka
type TelemetryRecord struct {
	BenchmarkRunID string `json:"benchmark_run_id"`
	OrderID        string `json:"order_id"`
	OrderType      string `json:"type"`
	LatencyNS      int64  `json:"latency_ns"`
	StatusCode     int    `json:"status_code"`
	IsSuccess      bool   `json:"is_success"`
	Timestamp      int64  `json:"timestamp"`
}

// Global shared HTTP client with connection pooling
var httpClient = &http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        20000,
		MaxIdleConnsPerHost: 20000,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false, // Re-use connections!
	},
}

var (
	kafkaWriter *kafka.Writer
	activeMutex sync.Mutex
	activeCancel context.CancelFunc
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}
	
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	log.Printf("Connecting to Kafka brokers at: %s", brokers)
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP(brokers),
		Topic:    "telemetry-stream",
		Balancer: &kafka.LeastBytes{},
		Async:    true, // Send asynchronously to prevent Kafka from slowing down the load test
	}
	defer kafkaWriter.Close()

	http.HandleFunc("/start", handleStart)
	http.HandleFunc("/stop", handleStop)

	log.Printf("🚀 Go Bot Fleet running on http://0.0.0.0:%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}

func handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req StartRequest
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.BenchmarkRunID == "" || req.TargetURL == "" || req.TPS <= 0 || req.Concurrency <= 0 {
		http.Error(w, "Missing or invalid parameters", http.StatusBadRequest)
		return
	}

	activeMutex.Lock()
	if activeCancel != nil {
		// Stop any ongoing test first
		activeCancel()
		log.Println("Stopping previous active stress test before starting new one.")
	}

	ctx, cancel := context.WithCancel(context.Background())
	activeCancel = cancel
	activeMutex.Unlock()

	// Spawn stress test runner in background
	go runStressTest(ctx, req)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"status":           "started",
		"benchmark_run_id": req.BenchmarkRunID,
	})
}

func handleStop(w http.ResponseWriter, r *http.Request) {
	activeMutex.Lock()
	if activeCancel != nil {
		activeCancel()
		activeCancel = nil
		log.Println("Stress test manual stop triggered.")
	}
	activeMutex.Unlock()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Stress test stopped"))
}

func runStressTest(ctx context.Context, req StartRequest) {
	log.Printf("[Run %s] Starting stress test. Target: %s, TPS: %d, Concurrency: %d, Duration: %ds",
		req.BenchmarkRunID, req.TargetURL, req.TPS, req.Concurrency, req.DurationSeconds)

	// Keep a list of generated order IDs to simulate cancellations
	var orderIDs []string
	var orderIDsMu sync.Mutex

	var wg sync.WaitGroup
	
	// Split target TPS across concurrent workers
	tpsPerWorker := float64(req.TPS) / float64(req.Concurrency)
	intervalPerWorker := time.Duration(float64(time.Second) / tpsPerWorker)

	for i := 0; i < req.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			// Introduce jitter to avoid workers firing at the exact same millisecond
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			
			ticker := time.NewTicker(intervalPerWorker)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					// Determine order action (80% LIMIT_ORDER, 10% MARKET_ORDER, 10% CANCEL_ORDER)
					roll := rand.Intn(100)
					var actionType string
					var responseCode int
					var isSuccess bool
					var orderID string
					tZero := time.Now().UnixNano()

					if roll < 80 {
						actionType = "LIMIT_ORDER"
						orderID = uuid.New().String()
						responseCode, isSuccess = createOrder(req.TargetURL, orderID, "LIMIT")
						if isSuccess {
							orderIDsMu.Lock()
							if len(orderIDs) < 1000 { // Cap memory cache size
								orderIDs = append(orderIDs, orderID)
							}
							orderIDsMu.Unlock()
						}
					} else if roll < 90 {
						actionType = "MARKET_ORDER"
						orderID = uuid.New().String()
						responseCode, isSuccess = createOrder(req.TargetURL, orderID, "MARKET")
					} else {
						actionType = "CANCEL_ORDER"
						orderIDsMu.Lock()
						if len(orderIDs) > 0 {
							// Pop random order ID
							idx := rand.Intn(len(orderIDs))
							orderID = orderIDs[idx]
							// Remove from list
							orderIDs[idx] = orderIDs[len(orderIDs)-1]
							orderIDs = orderIDs[:len(orderIDs)-1]
						}
						orderIDsMu.Unlock()

						if orderID != "" {
							responseCode, isSuccess = cancelOrder(req.TargetURL, orderID)
						} else {
							// If no orders to cancel, fallback to LIMIT_ORDER
							actionType = "LIMIT_ORDER"
							orderID = uuid.New().String()
							responseCode, isSuccess = createOrder(req.TargetURL, orderID, "LIMIT")
						}
					}

					tOne := time.Now().UnixNano()
					latency := tOne - tZero

					// Stream response telemetry to Kafka
					record := TelemetryRecord{
						BenchmarkRunID: req.BenchmarkRunID,
						OrderID:        orderID,
						OrderType:      actionType,
						LatencyNS:      latency,
						StatusCode:     responseCode,
						IsSuccess:      isSuccess,
						Timestamp:      tOne / 1e6, // millisecond timestamp
					}

					publishTelemetry(record)
				}
			}
		}(i)
	}

	// Wait for duration then cancel context
	select {
	case <-ctx.Done():
		log.Printf("[Run %s] Stress test aborted early.", req.BenchmarkRunID)
	case <-time.After(time.Duration(req.DurationSeconds) * time.Second):
		log.Printf("[Run %s] Stress test duration completed.", req.BenchmarkRunID)
		activeMutex.Lock()
		if activeCancel != nil {
			activeCancel()
			activeCancel = nil
		}
		activeMutex.Unlock()
	}

	wg.Wait()
	log.Printf("[Run %s] All workers exited.", req.BenchmarkRunID)

	// Callback to Core Orchestrator to signal run completion and trigger container shutdown
	notifyCompletion(req.BenchmarkRunID)
}

func createOrder(targetURL string, orderID string, orderType string) (int, bool) {
	side := "BUY"
	if rand.Intn(2) == 0 {
		side = "SELL"
	}
	
	payload := map[string]interface{}{
		"order_id": orderID,
		"type":     orderType,
		"side":     side,
		"price":    100.0 + (rand.Float64() * 20.0 - 10.0), // $90 - $110
		"quantity": rand.Intn(100) + 1,
	}

	jsonBytes, _ := json.Marshal(payload)
	
	req, err := http.NewRequest("POST", targetURL+"/order", bytes.NewBuffer(jsonBytes))
	if err != nil {
		return 0, false
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, false
	}
	
	// Read and discard body to reuse the TCP connection in pool
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	return resp.StatusCode, resp.StatusCode == 201 || resp.StatusCode == 200
}

func cancelOrder(targetURL string, orderID string) (int, bool) {
	req, err := http.NewRequest("DELETE", targetURL+"/order/"+orderID, nil)
	if err != nil {
		return 0, false
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, false
	}

	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	return resp.StatusCode, resp.StatusCode == 200
}

func publishTelemetry(record TelemetryRecord) {
	payload, err := json.Marshal(record)
	if err != nil {
		log.Printf("Error marshalling telemetry record: %v", err)
		return
	}

	msg := kafka.Message{
		Key:   []byte(record.BenchmarkRunID),
		Value: payload,
	}

	// WriteMessage sends message asynchronously to Kafka
	if err := kafkaWriter.WriteMessages(context.Background(), msg); err != nil {
		log.Printf("Error publishing telemetry message: %v", err)
	}
}

func notifyCompletion(runID string) {
	orchestratorURL := os.Getenv("ORCHESTRATOR_URL")
	if orchestratorURL == "" {
		orchestratorURL = "http://core-orchestrator:8000"
	}
	
	completeURL := orchestratorURL + "/benchmark/complete"
	log.Printf("[Completion] Calling Core Orchestrator at %s", completeURL)
	
	payload := map[string]string{
		"benchmark_run_id": runID,
	}
	jsonBytes, _ := json.Marshal(payload)
	
	resp, err := httpClient.Post(completeURL, "application/json", bytes.NewBuffer(jsonBytes))
	if err != nil {
		log.Printf("[Completion] Failed to notify Core Orchestrator: %v", err)
		return
	}
	resp.Body.Close()
	log.Printf("[Completion] Callback successful. Status: %s", resp.Status)
}
