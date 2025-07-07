package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/IBM/sarama"
	"github.com/oschwald/geoip2-golang"
	"github.com/redis/go-redis/v9"
)

// Transaction represents a financial transaction enriched with location
type Transaction struct {
	TransactionID string    `json:"transaction_id"`
	UserID        string    `json:"user_id"`
	Amount        float64   `json:"amount"`
	IPAddress     string    `json:"ip_address"`
	Timestamp     time.Time `json:"timestamp"`
	Country       string    `json:"country,omitempty"`
	City          string    `json:"city,omitempty"`
}

// ConsumerGroupHandler processes messages from Kafka
type ConsumerGroupHandler struct {
	producer sarama.SyncProducer
	db       *geoip2.Reader
	redis    *redis.Client
}

func (h ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := context.Background()

	for kafkaMsg := range claim.Messages() {
		var txn Transaction
		err := json.Unmarshal(kafkaMsg.Value, &txn)
		if err != nil {
			log.Printf("Failed to unmarshal: %v", err)
			continue
		}

		ip := net.ParseIP(txn.IPAddress)
		if ip != nil {
			cacheKey := txn.IPAddress

			// Try Redis first
			locationJSON, err := h.redis.Get(ctx, cacheKey).Result()
			if err == nil {
				// Redis cache hit
				var loc map[string]string
				if jsonErr := json.Unmarshal([]byte(locationJSON), &loc); jsonErr == nil {
					txn.Country = loc["country"]
					txn.City = loc["city"]
				}
			} else {
				// Redis cache miss — use GeoIP
				if record, err := h.db.City(ip); err == nil {
					txn.Country = record.Country.Names["en"]
					txn.City = record.City.Names["en"]

					// Save to Redis
					locMap := map[string]string{
						"country": txn.Country,
						"city":    txn.City,
					}
					locJSON, _ := json.Marshal(locMap)
					h.redis.Set(ctx, cacheKey, locJSON, 24*time.Hour)
				}
			}
		}

		enrichedJSON, err := json.Marshal(txn)
		if err != nil {
			log.Printf("Failed to marshal enriched txn: %v", err)
			continue
		}

		// Produce to enriched_transactions topic
		producerMsg := &sarama.ProducerMessage{
			Topic: "enriched_transactions",
			Value: sarama.ByteEncoder(enrichedJSON),
		}
		_, _, err = h.producer.SendMessage(producerMsg)
		if err != nil {
			log.Printf("Failed to send enriched message: %v", err)
		} else {
			fmt.Printf("Enriched txn sent for %s (IP: %s → %s, %s)\n", txn.TransactionID, txn.IPAddress, txn.City, txn.Country)
		}

		// Mark message as processed
		session.MarkMessage(kafkaMsg, "")
	}

	return nil
}

func main() {
	brokers := []string{"localhost:9092"}
	groupID := "enricher-group"
	sourceTopic := "raw_transactions"
	geoDBPath := "GeoLite2-City.mmdb"

	// Load GeoLite2 DB
	db, err := geoip2.Open(geoDBPath)
	if err != nil {
		log.Fatalf("Could not open GeoLite2 DB: %v", err)
	}
	defer db.Close()

	// Initialize Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer redisClient.Close()

	// Kafka config
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Producer.Return.Successes = true

	// Create Kafka consumer group
	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("Could not create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	// Create Kafka producer
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Could not create producer: %v", err)
	}
	defer producer.Close()

	handler := ConsumerGroupHandler{
		producer: producer,
		db:       db,
		redis:    redisClient,
	}

	// Handle interrupt signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, os.Interrupt)
		<-sigchan
		fmt.Println("\nInterrupt received. Exiting...")
		cancel()
	}()

	fmt.Println("Enrichment consumer started. Waiting for messages...")

	for {
		if err := consumerGroup.Consume(ctx, []string{sourceTopic}, handler); err != nil {
			log.Printf("Error during consume: %v\n", err)
			time.Sleep(1 * time.Second)
		}
		if ctx.Err() != nil {
			break
		}
	}

	fmt.Println("Consumer shut down.")
}
