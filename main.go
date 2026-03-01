package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Message struct {
	ID   string `json:"id"`
	Body string `json:"body"`
}

var (
	jobQueue = make(chan Message)
	wg       sync.WaitGroup
)

func worker(id int) {
	defer wg.Done()

	for msg := range jobQueue {
		log.Printf("Worker %d processing message %s\n", id, msg.ID)
		time.Sleep(2 * time.Second)
	}

	log.Printf("Worker %d shutting down\n", id)
}

func messageHandler(writer http.ResponseWriter, req *http.Request) {
	var msg Message
	err := json.NewDecoder(req.Body).Decode(&msg)

	if err != nil {
		http.Error(writer, "Invalid request", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(req.Context(), 3*time.Second)
	defer cancel()

	select {
	case jobQueue <- msg:
		writer.WriteHeader(http.StatusAccepted)
		err2 := json.NewEncoder(writer).Encode(map[string]string{"status": "queued"})
		if err2 != nil {
			log.Printf("Json Encoder error: %v", err)
		}
	case <-ctx.Done():
		// Timeout
		http.Error(writer, "System overloaded, try again latter", http.StatusServiceUnavailable)
	}
}

func main() {
	_, cancel := context.WithCancel(context.Background())

	// Add 3 workers
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go worker(i)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/message", messageHandler)

	server := http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Printf("Server running on port 8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Listen error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Printf("Shutting down...")

	cancel()

	err := server.Shutdown(context.Background())
	if err != nil {
		log.Printf("Shutdown error: %v", err)
	}

	close(jobQueue)
	wg.Wait()
}
