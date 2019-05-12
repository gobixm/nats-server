package server

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"golang.org/x/crypto/bcrypt"
	"math/rand"
	"sync"
)

type KafkaClientAuthentication struct
{
	brokerList string
	topic string
	users *sync.Map
}

func (auth KafkaClientAuthentication) Check(c ClientAuthentication) bool {
	opts := c.GetOpts()
 	username := opts.Username
 	password := opts.Password

	user, exists := auth.users.Load(username)

	if !exists {
		return false
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.(*User).Password), []byte(password)); err != nil {
		return false
	}

	c.RegisterUser(user.(*User))
	return true
}

func (auth KafkaClientAuthentication) Configure() {

}

func NewKafkaAuth(opts *Options) *KafkaClientAuthentication {
	return &KafkaClientAuthentication{
		brokerList: opts.KafkaBrokers,
		topic: opts.KafkaTopic,
		users: &sync.Map{},
	}
}

func (auth KafkaClientAuthentication) Start() {
	go func() {
		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": auth.brokerList,
			"auto.offset.reset": "earliest",
			"group.id": fmt.Sprintf("nats-kafka-auth-%d", rand.Int31()),
		})

		if err != nil {
			panic(err)
		}

 		c.Subscribe(auth.topic, nil)

		for {
			msg, err := c.ReadMessage(-1)
			if err == nil {
				username := string(msg.Key)

				if msg.Value == nil {
					auth.users.Delete(username)
					continue
				}

				user := &User{}
				err = json.Unmarshal([]byte(msg.Value), &user)
				if err != nil {
					// Error parsing permissions
					continue
				}


				user.Username = username
				auth.users.Store(username, user)
			} else {
 				fmt.Printf("KafkaClientAuthentication consumer error: %v (%v)\n", err, msg)
			}
		}

		c.Close()
	}()
}
