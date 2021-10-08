package main

import (
	"io/ioutil"
	"log"
	"net/http"

	"github.com/ariefmaulidy/security-workshop/encryption"
	"github.com/ariefmaulidy/security-workshop/messaging"
	jsoniter "github.com/json-iterator/go"
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
		Topic:         "payment",
		Channel:       "insert",
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
	var err error

	responseBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Print(err)
	}

	// make sure data struct is valid
	var payment Payment
	err = jsoniter.Unmarshal(responseBody, &payment)
	if err != nil {
		log.Print(err)
	}

	securePayment, err := encryption.EncrpytRSA(encryption.ServiceB, responseBody)
	if err != nil {
		log.Print(err)
	}

	securePayment, err = encryption.EncryptAES(encryption.AESKey, []byte(securePayment))
	if err != nil {
		log.Print(err)
	}

	// Do Publish
	producer.Publish("payment", securePayment)
}

func handleMessage(message *nsq.Message) error {
	var err error

	messageData, err := encryption.DecryptAES(encryption.AESKey, string(message.Body))
	if err != nil {
		log.Print(err)
	}

	messageData, err = encryption.DecryptRSA(encryption.ServiceB, string(messageData))
	if err != nil {
		log.Print(err)
	}

	// Handle message NSQ here
	var payment Payment
	err = jsoniter.Unmarshal(messageData, &payment)
	if err != nil {
		log.Print(err)
	}

	message.Finish()
	return nil
}
