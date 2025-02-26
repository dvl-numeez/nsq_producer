package main

import (
	"log"

	"github.com/nsqio/go-nsq"
)

func main() {
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer("127.0.0.1:4150", config)
	if err != nil {
		log.Fatal(err)
	}

	err = producer.Publish("NSQ_TEST", []byte(`{
	"name":"digivatelabs"
	}`))
	if err != nil {
		log.Fatal(err)
	}

	defer producer.Stop()

}
