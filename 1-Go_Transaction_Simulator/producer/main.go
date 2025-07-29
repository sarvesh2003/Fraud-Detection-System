package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

var topicName = "raw_transactions"
var userIDs []string

// Transaction struct aligned with your ML features
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
}

// Indian ISP IP ranges
var ispIPRanges = map[string][]string{
	"IN-Bangalore": {"116.119.0.0/16", "182.64.0.0/14", "49.206.0.0/16", "49.207.0.0/16"},
	"IN-Delhi":     {"115.248.0.0/16", "117.192.0.0/14", "106.51.0.0/16", "122.180.0.0/16"},
	"IN-Mumbai":    {"115.96.0.0/14", "115.112.0.0/13", "116.118.0.0/16", "116.119.0.0/16"},
	"IN-Chennai":   {"122.178.0.0/16", "122.179.0.0/16", "103.226.0.0/16", "103.227.0.0/16"},
}

var transactionTypes = []string{"PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"}
var typeEncoding = map[string]int{
	"PAYMENT":   0,
	"TRANSFER":  1,
	"CASH_OUT":  2,
	"DEBIT":     3,
	"CASH_IN":   4,
}

var userRegion = map[string]string{}
var userIPMap sync.Map
var regions = []string{"IN-Bangalore", "IN-Delhi", "IN-Mumbai", "IN-Chennai"}

func setupRegionsAndUsers() {
	userCountPerRegion := 100
	for _, region := range regions {
		for i := 1; i <= userCountPerRegion; i++ {
			userID := fmt.Sprintf("%s_user_%03d", region[:2], i)
			userRegion[userID] = region
		}
	}
}

func generateIPFromCIDR(cidr string) string {
	_, ipnet, _ := net.ParseCIDR(cidr)
	ip := make(net.IP, len(ipnet.IP))
	copy(ip, ipnet.IP)
	for i := 0; i < len(ip)-len(ipnet.Mask); i++ {
		ip[len(ipnet.IP)+i] += byte(rand.Intn(255))
	}
	return ip.String()
}

func getRandomIPForRegion(region string) string {
	ranges := ispIPRanges[region]
	cidr := ranges[rand.Intn(len(ranges))]
	return generateIPFromCIDR(cidr)
}

func getUserIP(userID string) string {
	region := userRegion[userID]
	if val, exists := userIPMap.Load(userID); exists && rand.Float64() < 0.9 {
		return val.(string)
	}
	newIP := getRandomIPForRegion(region)
	userIPMap.Store(userID, newIP)
	return newIP
}

func generateAmount() float64 {
	r := rand.Float64()
	if r < 0.9 {
		return float64(rand.Intn(20000-20) + 20)
	}
	return float64(rand.Intn(1000000000-20000) + 20000)
}

func generateRandomTransaction() Transaction {
	transactionID := "txn_" + uuid.New().String()
	userID := userIDs[rand.Intn(len(userIDs))]
	amount := generateAmount()
	ipAddress := getUserIP(userID)
	timestamp := time.Now().UTC()

	txType := transactionTypes[rand.Intn(len(transactionTypes))]
	typeEncoded := typeEncoding[txType]

	oldOrig := float64(rand.Intn(1000000) + 1000)
	newOrig := oldOrig - amount
	if newOrig < 0 {
		newOrig = 0
	}

	oldDest := float64(rand.Intn(1000000) + 1000)
	newDest := oldDest + amount

	return Transaction{
		TransactionID:  transactionID,
		UserID:         userID,
		Amount:         amount,
		Type:           txType,
		TypeEncoded:    typeEncoded,
		OldBalanceOrig: oldOrig,
		NewBalanceOrig: newOrig,
		OldBalanceDest: oldDest,
		NewBalanceDest: newDest,
		DeltaOrig:      newOrig - oldOrig,
		DeltaDest:      newDest - oldDest,
		IPAddress:      ipAddress,
		Timestamp:      timestamp,
	}
}

func connectKafkaProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	return sarama.NewSyncProducer(brokers, config)
}

func simulateProducer(id int, interval time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()
	producer, err := connectKafkaProducer([]string{"localhost:9092"})
	if err != nil {
		log.Fatalf("Producer %d - Kafka connection error: %v", id, err)
	}
	defer producer.Close()

	for {
		transaction := generateRandomTransaction()
		transactionJSON, err := json.Marshal(transaction)
		if err != nil {
			log.Printf("Producer %d - JSON marshal error: %v", id, err)
			continue
		}

		msg := &sarama.ProducerMessage{
			Topic: topicName,
			Value: sarama.StringEncoder(transactionJSON),
		}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("Producer %d - Kafka send error: %v", id, err)
		} else {
			log.Printf("Producer %d - Sent to %s [partition %d, offset %d]", id, topicName, partition, offset)
		}

		time.Sleep(interval)
	}
}

func getAllUserIDs() []string {
	userList := []string{}
	for user := range userRegion {
		userList = append(userList, user)
	}
	return userList
}

func main() {
	rand.Seed(time.Now().UnixNano())
	setupRegionsAndUsers()
	userIDs = getAllUserIDs()

	var wg sync.WaitGroup
	wg.Add(3)
	go simulateProducer(1, 1*time.Second, &wg)
	go simulateProducer(2, 2*time.Second, &wg)
	go simulateProducer(3, 3*time.Second, &wg)
	wg.Wait()
}