package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/ariefmaulidy/security-workshop/encryption"
	"github.com/ariefmaulidy/security-workshop/messaging"
	"github.com/nsqio/go-nsq"
)

var (
	producer messaging.Producer
	key      []byte
)

const (
	defaultConsumerMaxAttempts = 10
	defaultConsumerMaxInFlight = 100
)

type User struct {
	Name        string `json:"name"`
	Email       string `json:"email"`
	PhoneNumber string `json:"phone_number"`
	Age         int    `json:"age"`
}

type Item struct {
	Name       string `json:"name"`
	Price      int64  `json:"price"`
	Category   string `json:"category"`
	Quantity   int    `json:"quantity"`
	TotalPrice int64  `json:"total_price"`
}

type Payment struct {
	UserData     User   `json:"user_data"`
	ItemData     []Item `json:"item_data"`
	TotalPayment int64  `json:"total_payment"`
}

func main() {
	config := nsq.NewConfig()
	config.MaxAttempts = 200

	var err error
	producer, err = messaging.NewProducer(messaging.ProducerConfig{
		NsqdAddress: "172.18.59.254:4150",
	})
	if err != nil {
		log.Fatal("Failed init producer", err)
	}

	// initiate consumer
	consumer, err := messaging.NewConsumer(messaging.ConsumerConfig{
		Topic:         "fasma_security_workshop_del_soon", // Change the topic
		Channel:       "fasma_security_workshop_del_soon", // Change the channel
		LookupAddress: "172.18.59.254:4161",
		MaxAttempts:   defaultConsumerMaxAttempts,
		MaxInFlight:   defaultConsumerMaxInFlight,
		Handler:       handleMessage,
	})
	if err != nil {
		log.Fatal("Failed init consumer", err)
	}

	http.HandleFunc("/publish_payment", handlePublish)

	go messaging.RunConsumer(consumer)
	log.Fatal(http.ListenAndServe(":8090", nil))
}

func handlePublish(w http.ResponseWriter, r *http.Request) {
	// Do Publish
	topic := "fasma_security_workshop_del_soon"
	msg := Payment{
		UserData: User{
			Name:        "Fasma",
			Email:       "marde.aza@tokopedia.com",
			PhoneNumber: "088888888",
			Age:         22,
		},
		ItemData: []Item{
			{
				Name:       "Simba",
				Price:      50000,
				Category:   "cereal",
				Quantity:   2,
				TotalPrice: 100000,
			},
			{
				Name:       "Coco Crunch",
				Price:      20000,
				Category:   "cereal",
				Quantity:   4,
				TotalPrice: 80000,
			},
		},
		TotalPayment: 180000,
	}
	fmt.Printf("[SERVICE A][Payload] %+v\n", msg)

	msgByte, err := json.Marshal(msg)
	if err != nil {
		log.Fatal("Failed marshal message", err)
	}
	fmt.Printf("[SERVICE A][After Marshal] %+v\n", msgByte)

	msgEncrypted, err := encryption.EncrpytRSA(encryption.ServiceA, msgByte)
	if err != nil {
		log.Fatal("Failed encryption", err)
	}
	fmt.Printf("[SERVICE A][After Encrypt] %+v\n", msgEncrypted)

	producer.Publish(topic, msgEncrypted)
}

func handleMessage(message *nsq.Message) error {
	fmt.Printf("[SERVICE B][Input Message] %+v\n", message.Body)

	msgDecrypted, err := encryption.DecryptRSA(encryption.ServiceA, string(message.Body))
	if err != nil {
		log.Fatal("Failed decryption", err)
	}
	fmt.Printf("[SERVICE B][After Decrypt] %+v\n", msgDecrypted)

	payload := Payment{}
	err = json.Unmarshal(msgDecrypted, &payload)
	if err != nil {
		log.Fatal("Failed unmarshal message", err)
	}

	fmt.Printf("[SERVICE B][After Unmarshal] %+v\n", payload)

	message.Finish()
	return nil
}
