package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/nsqio/go-nsq"

	"github.com/ariefmaulidy/security-workshop/encryption"
	"github.com/ariefmaulidy/security-workshop/messaging"
)

var (
	producer messaging.Producer
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

type EncryptedData struct {
	Data string `json:"data"`
	Key  string `json:"key"`
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
		Topic:         "test_idzhar", // Change the topic
		Channel:       "test_idzhar", // Change the channel
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
	p := Payment{
		UserData: User{
			Name:        "asdasdf",
			Email:       "asdasd",
			PhoneNumber: "12312312",
			Age:         123,
		},
		ItemData: []Item{
			{
				Name:       "asdasdsa",
				Price:      123123,
				Category:   "123123",
				Quantity:   1230,
				TotalPrice: 120,
			},
		},
		TotalPayment: 1231230,
	}

	msg, err := json.Marshal(p)
	if err != nil {
		fmt.Println("ERROR:", err)
		return
	}

	topic := "test_idzhar"
	key := hex.EncodeToString([]byte(RandStringRunes(16)))

	keyEncrypted, err := encryption.EncrpytRSA(encryption.ServiceB, []byte(key))
	if err != nil {
		fmt.Println("ERROR:", err)
	}

	cipher, err := encryption.EncryptAES(key, msg)

	d := EncryptedData{
		Data: cipher,
		Key:  keyEncrypted,
	}

	err = producer.Publish(topic, d)
	if err != nil {
		fmt.Println("ERROR:", err)
	}
}

func handleMessage(message *nsq.Message) error {
	// Handle message NSQ here

	var msg EncryptedData
	err := json.Unmarshal(message.Body, &msg)
	if err != nil {
		fmt.Println("ERROR:", err)
		return nil
	}

	key, err := encryption.DecryptRSA(encryption.ServiceB, msg.Key)
	fmt.Println("KEY:", key)
	plainText, err := encryption.DecryptAES(string(key), string(msg.Data))
	if err != nil {
		fmt.Println("ERROR:", err)
		return nil
	}

	p := Payment{}
	err = json.Unmarshal(plainText, &p)

	fmt.Println("Recv message:", p)

	message.Finish()
	return nil
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
