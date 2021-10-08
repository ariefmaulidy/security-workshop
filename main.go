package main

import (
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
		Topic:         "bgp_tech_cur_test_zahrah", // Change the topic
		Channel:       "bgp_tech_cur_test_zahrah", // Change the channel
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
	log.Fatal(http.ListenAndServe(":8191", nil))
}

func handlePublish(w http.ResponseWriter, r *http.Request) {
	// Do Publish
	topic := "bgp_tech_cur_test_zahrah" // TODO: update to given topic name
	pymData := Payment{
		UserData: User{
			Name: "Zahrah",
		},
		ItemData: []Item{
			{
				Name:       "Mug 350 ml",
				Price:      30000,
				TotalPrice: 30000,
			},
		},
		TotalPayment: 30000,
	}
	str, _ := jsoniter.MarshalToString(pymData)

	msg, err := encryption.EncrpytRSA(encryption.ServiceB, []byte(str)) // TODO: write your message here
	if err != nil {
		log.Println("Failed to encrypt data")
	}

	log.Println("Sending data to NSQ")
	producer.Publish(topic, msg)
}

func handleMessage(message *nsq.Message) error {
	// Handle message NSQ here

	log.Println("Incoming payment data")
	decryptedMsg, err := encryption.DecryptRSA(encryption.ServiceB, string(message.Body))
	if err != nil {
		log.Println("Failed to decrypt data")
	}
	log.Println(string(decryptedMsg))
	message.Finish()
	return nil
}
