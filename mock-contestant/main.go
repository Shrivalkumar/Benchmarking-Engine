package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

type OrderRequest struct {
	OrderID  string  `json:"order_id"`
	Type     string  `json:"type"` // LIMIT or MARKET
	Side     string  `json:"side"` // BUY or SELL
	Price    float64 `json:"price"`
	Quantity int     `json:"quantity"`
}

type OrderResponse struct {
	OrderID        string `json:"order_id"`
	Status         string `json:"status"` // filled, partially_filled, cancelled, active
	QuantityFilled int    `json:"quantity_filled"`
}

func main() {
	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/order", handleOrder)
	http.HandleFunc("/order/", handleCancelOrder) // Matches /order/{id}

	log.Println("🚀 Mock Matching Engine running on http://0.0.0.0:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

func handleOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}

	if req.OrderID == "" || req.Type == "" || req.Side == "" || req.Quantity <= 0 {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// Simulate processing time of a real matching engine (0.2ms to 1.5ms)
	latency := 200 + rand.Intn(1300)
	time.Sleep(time.Duration(latency) * time.Microsecond)

	status := "active"
	filledQty := 0
	if req.Type == "MARKET" {
		status = "filled"
		filledQty = req.Quantity
	} else {
		// LIMIT orders match 70% of the time in this simulation
		if rand.Intn(100) < 70 {
			status = "filled"
			filledQty = req.Quantity
		}
	}

	resp := OrderResponse{
		OrderID:        req.OrderID,
		Status:         status,
		QuantityFilled: filledQty,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func handleCancelOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract order ID from path `/order/{id}`
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 3 || parts[2] == "" {
		http.Error(w, "Missing order ID", http.StatusBadRequest)
		return
	}
	orderID := parts[2]

	// Simulate cancel order processing latency
	latency := 100 + rand.Intn(500)
	time.Sleep(time.Duration(latency) * time.Microsecond)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"order_id": orderID,
		"status":   "cancelled",
	})
}
