package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	//  []byte("transferencia") - key, garante que as mensagens sempre chegarao na mesma partição
	Publish("transferir", "teste", producer, []byte("transferencia2"), deliveryChan)
	go DeliveryReport(deliveryChan) // async
	producer.Flush(1000)

	// e := <-deliveryChan
	// msg := e.(*kafka.Message)

	// if msg.TopicPartition.Error != nil {
	// 	log.Println("Erro ao enviar: ", msg.TopicPartition.Error)
	// } else {
	// 	log.Println("Mensagem enviada: ", msg.TopicPartition)
	// }

}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "kafka:9092",
		"delivery.timeout.ms": "0",
		"acks":                "all",  // 0 nao precisa receber retorno, 1 aguarda que pelo menos um leader respondeu, all leader e brokers respondeem
		"enable.idempotence":  "true", // false por padrao, se for true o acks tem que ser all
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}

	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Println("Erro ao enviar: ", ev.TopicPartition)
			} else {
				log.Println("Mensagem enviada: ", ev.TopicPartition)
				// anotar no bd que a msg foi enviada
				// ex: confirma que uma transação bancaria ocorreu
			}
		}
	}
}
