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
)

// Expected structure of the modified transaction which will be pushed to enriched_transactions topic
type Transaction struct {
	TransactionID string    `json:"transaction_id"`
	UserID        string    `json:"user_id"`
	Amount        float64   `json:"amount"`
	IPAddress     string    `json:"ip_address"`
	Timestamp     time.Time `json:"timestamp"`
	Country       string    `json:"country,omitempty"`
	City          string    `json:"city,omitempty"`
}

type ConsumerGroupHandler struct {
	producer sarama.SyncProducer
	db       *geoip2.Reader
}

func (h ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for kafkaMsg := range claim.Messages() {
		var txn Transaction
		err := json.Unmarshal(kafkaMsg.Value, &txn)
		if err != nil {
			log.Printf("❌ Failed to unmarshal: %v", err)
			continue
		}

		ip := net.ParseIP(txn.IPAddress)
		if ip != nil {
			if record, err := h.db.City(ip); err == nil {
				txn.Country = record.Country.Names["en"]
				if len(record.City.Names["en"]) > 0 {
					txn.City = record.City.Names["en"]
				}
			}
		}

		enrichedJSON, err := json.Marshal(txn)
		if err != nil {
			log.Printf("❌ Failed to marshal enriched txn: %v", err)
			continue
		}

		// Push to enriched_transactions topic
		producerMsg := &sarama.ProducerMessage{
			Topic: "enriched_transactions",
			Value: sarama.ByteEncoder(enrichedJSON),
		}
		_, _, err = h.producer.SendMessage(producerMsg)
		if err != nil {
			log.Printf("❌ Failed to send enriched message: %v", err)
		} else {
			fmt.Printf("✅ Enriched txn sent for %s (IP: %s -> %s, %s)\n", txn.TransactionID, txn.IPAddress, txn.City, txn.Country)
		}

		// ✅ Correct usage: mark the original Kafka message
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
		log.Fatalf("❌ Could not open GeoLite2 DB: %v", err)
	}
	defer db.Close()

	// Kafka config
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Producer.Return.Successes = true

	// Create consumer group
	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("❌ Could not create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	// Create producer
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("❌ Could not create producer: %v", err)
	}
	defer producer.Close()

	handler := ConsumerGroupHandler{
		producer: producer,
		db:       db,
	}

	// Handle shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, os.Interrupt)
		<-sigchan
		fmt.Println("\n🛑 Interrupt received. Exiting...")
		cancel()
	}()

	fmt.Println("🚀 Enrichment consumer started. Waiting for messages...")

	// Keep consuming
	for {
		if err := consumerGroup.Consume(ctx, []string{sourceTopic}, handler); err != nil {
			log.Printf("⚠️ Error during consume: %v\n", err)
			time.Sleep(1 * time.Second)
		}
		if ctx.Err() != nil {
			break
		}
	}

	fmt.Println("✅ Consumer shut down.")
}
