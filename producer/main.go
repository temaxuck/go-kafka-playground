package main

import (
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
	})

	if err != nil {
		panic(fmt.Sprintf("Failed to create producer: %v", err))
	}

	defer p.Close()

	var wg sync.WaitGroup

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
					wg.Done()
				}
			}
		}
	}()

	topic := "test-topic"

	for _, word := range []string{"Welcome", "to", "the", "feast", "mansion"} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte("not-messages"),
			Value: []byte(word),
		}, nil)
		wg.Add(1)
	}

	wg.Wait()

	p.Flush(15 * 1000)
}
