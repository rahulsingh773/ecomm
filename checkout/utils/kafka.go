package utils

import (
	"fmt"

	"github.com/confluent-kafka-go/kafka"
)

var product_producer *kafka.Producer
var product_client, payment_client *kafka.Consumer

func init() {
	NewProducer(product_producer)
	NewConsumer(product_client, "products")
	NewConsumer(payment_client, "payment")
}

func NewConsumer(client *kafka.Consumer, topic string) {
	var err error
	client, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	client.SubscribeTopics([]string{topic, "^aRegex.*[Tt]opic"}, nil)
}

func NewProducer(producer *kafka.Producer) {
	var err error
	producer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		fmt.Printf("error while creating producer: %v\n", err)
		panic(err)
	}

	// Delivery report handler for produced messages
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to %v\n", ev)
				}
			}
		}
	}()
}

func PublishKafkaEvent(event string) error {
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(event),
	}, nil)

	return nil
}

func ConsumeEvent(client *kafka.Consumer) string {
	msg, err := client.ReadMessage(-1)
	if err == nil {
		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
	} else {
		fmt.Printf("Consumer error: %v (%v)\n", err, msg)
	}

	return msg
}
