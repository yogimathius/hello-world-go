package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	// "strings"
	"time"

	"github.com/IBM/sarama"
)


type Message struct {
	EventType string    `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`
	Priority  string    `json:"priority"`
	Source    string    `json:"source"`
	Location  string    `json:"location"`
}

var (
	brokers  = []string{"localhost:9092"} // Kafka broker addresses
	topic    = "hello-world-topic"        // Kafka topic to produce to
	producer sarama.SyncProducer          // Kafka producer instance
)

func main() {
	// Initialize Kafka producer
	initKafkaProducer()

	// Define the HTTP handler function for POST requests to /message
	http.HandleFunc("/message", handleMessage)

	// Start the HTTP server on port 8080
	fmt.Println("Server listening on port 8080...")
	go func() {
		err := http.ListenAndServe(":8080", nil)
		if err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Graceful shutdown handling
	handleShutdown()
}

func initKafkaProducer() {
	// Configure Kafka producer
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to respond
	// config.Producer.Compression = sarama.CompressionSnappy    // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
	config.Producer.Return.Successes = true                  // Enable success notifications

	// Initialize the producer
	var err error
	producer, err = sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Error creating Kafka producer: %v", err)
	}

	// Handle successful startup
	fmt.Println("Kafka producer initialized")
}

func handleMessage(w http.ResponseWriter, r *http.Request) {
	// Check if the HTTP method is POST
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode the JSON request body into a Message struct
	var msg Message
	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	// Print the received message to console (for demonstration)
	fmt.Printf("Received message: %s\n", msg)

	// Produce message to Kafka topic
	sendMessageToKafka(msg)

	// Send a response back
	response := map[string]string{"message": "Message sent to Kafka successfully"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func sendMessageToKafka(message Message) {
	jsonMessage, err := json.Marshal(message)
	if err != nil {
		log.Printf("Failed to marshal message to JSON: %v\n", err)
		return
	}

	partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(jsonMessage),
	})
	if err != nil {
		log.Printf("Failed to send message to Kafka: %v\n", err)
		return
	}
	fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}

func handleShutdown() {
	// Handle graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	// Block until a signal is received
	<-stop
	fmt.Println("\nShutting down...")

	// Close Kafka producer
	if err := producer.Close(); err != nil {
		log.Printf("Error closing Kafka producer: %v\n", err)
	}

	// Close HTTP server
	fmt.Println("HTTP server shutdown")
	os.Exit(0)
}
