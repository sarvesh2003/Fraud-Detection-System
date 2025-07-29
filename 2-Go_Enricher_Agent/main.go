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

// Transaction includes enriched fields + original payload from producer
type Transaction struct {
	TransactionID   string    `json:"transaction_id"`
	UserID          string    `json:"user_id"`
	Amount          float64   `json:"amount"`
	Type            string    `json:"type"`
	TypeEncoded     int       `json:"type_encoded"`
	OldBalanceOrig  float64   `json:"oldbalanceOrg"`
	NewBalanceOrig  float64   `json:"newbalanceOrig"`
	OldBalanceDest  float64   `json:"oldbalanceDest"`
	NewBalanceDest  float64   `json:"newbalanceDest"`
	DeltaOrig       float64   `json:"delta_orig"`
	DeltaDest       float64   `json:"delta_dest"`
	IPAddress       string    `json:"ip_address"`
	Timestamp       time.Time `json:"timestamp"`

	// Enriched fields
	Country string `json:"country,omitempty"`
	City    string `json:"city,omitempty"`
}

// ConsumerGroupHandler processes raw transactions, enriches, and re-publishes
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
			log.Printf("Failed to unmarshal transaction: %v", err)
			continue
		}

		// Enrich IP with city/country using Redis or GeoIP
		ip := net.ParseIP(txn.IPAddress)
		if ip != nil {
			cacheKey := txn.IPAddress

			// Try Redis cache first
			if locationJSON, err := h.redis.Get(ctx, cacheKey).Result(); err == nil {
				var loc map[string]string
				if jsonErr := json.Unmarshal([]byte(locationJSON), &loc); jsonErr == nil {
					txn.Country = loc["country"]
					txn.City = loc["city"]
				}
			} else {
				// Redis miss → use GeoIP lookup
				if record, err := h.db.City(ip); err == nil {
					txn.Country = record.Country.Names["en"]
					txn.City = record.City.Names["en"]

					// Save to Redis cache
					locMap := map[string]string{
						"country": txn.Country,
						"city":    txn.City,
					}
					if locJSON, err := json.Marshal(locMap); err == nil {
						h.redis.Set(ctx, cacheKey, locJSON, 24*time.Hour)
					}
				}
			}
		}

		// Marshal enriched JSON
		enrichedJSON, err := json.Marshal(txn)
		if err != nil {
			log.Printf("Failed to marshal enriched txn: %v", err)
			continue
		}

		// Produce enriched transaction to new topic
		producerMsg := &sarama.ProducerMessage{
			Topic: "enriched_transactions",
			Value: sarama.ByteEncoder(enrichedJSON),
		}
		_, _, err = h.producer.SendMessage(producerMsg)
		if err != nil {
			log.Printf("Failed to send enriched message: %v", err)
		} else {
			fmt.Printf("Enriched txn: %s | IP: %s → %s, %s\n", txn.TransactionID, txn.IPAddress, txn.City, txn.Country)
		}

		session.MarkMessage(kafkaMsg, "")
	}

	return nil
}

func main() {
	brokers := []string{"localhost:9092"}
	groupID := "enricher-group"
	sourceTopic := "raw_transactions"
	geoDBPath := "GeoLite2-City.mmdb"

	// Load MaxMind DB
	db, err := geoip2.Open(geoDBPath)
	if err != nil {
		log.Fatalf("******** Failed to load GeoLite2 DB: %v ********", err)
	}
	defer db.Close()

	// Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer redisClient.Close()

	// Kafka configuration
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Producer.Return.Successes = true

	// Consumer group
	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("********** Failed to create consumer group: %v **********", err)
	}
	defer consumerGroup.Close()

	// Producer
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("********** Failed to create producer: %v **********", err)
	}
	defer producer.Close()

	handler := ConsumerGroupHandler{
		producer: producer,
		db:       db,
		redis:    redisClient,
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, os.Interrupt)
		<-sigchan
		fmt.Println("\n Interrupt signal received. Shutting down...")
		cancel()
	}()

	fmt.Println("Enricher agent started. Waiting for messages from raw_transactions...")

	for {
		if err := consumerGroup.Consume(ctx, []string{sourceTopic}, handler); err != nil {
			log.Printf("Error during consumption: %v\n", err)
			time.Sleep(1 * time.Second)
		}
		if ctx.Err() != nil {
			break
		}
	}

	fmt.Println("********** Enricher agent stopped. **********")
}