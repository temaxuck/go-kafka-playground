package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"group.id":          "JojiRichBrian",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	defer c.Close()

	// err = c.SubscribeTopics([]string{"test-topic", "^aRegex.*[Tt]opic"}, nil)
	err = c.Assign([]kafka.TopicPartition{
		{Topic: &[]string{"test-topic"}[0], Partition: 0},
	})

	if err != nil {
		panic(err)
	}

	run := true

	for run {
		msg, err := c.ReadMessage(time.Millisecond)
		if err == nil {
			if string(msg.Key) == "messages" {
				fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			} else {
				fmt.Printf("Unreachable key on %s: %s\n", msg.TopicPartition, string(msg.Key))
			}

		} else if !err.(kafka.Error).IsTimeout() {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}
