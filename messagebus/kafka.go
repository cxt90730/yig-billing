package messagebus

import (
	"bytes"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	. "github.com/journeymidnight/yig-billing/helper"
	"github.com/ugorji/go/codec"
)

func NewConsumer() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": Conf.KafkaServer,
		"group.id":          Conf.KafkaGroupId,
		"auto.offset.reset": "earliest"})
	if err != nil {
		Logger.Println("[ERROR] New Consumer failed with:", err)
		panic(err)
	}
	err = consumer.Subscribe(Conf.KafkaTopic, nil)
	if err != nil {
		Logger.Println("[ERROR] New Consumer with topic err:", err)
		panic(err)
	}
	KafkaConsumer = new(Kafka)
	KafkaConsumer.consumer = consumer
	ConsumerMessagePipe = make(chan ConsumerMessage)
	Logger.Println("[INFO] New Consumer successful!")
}

func StartConsumerReceiver() {
	for {
		msg, err := KafkaConsumer.consumer.ReadMessage(-1)
		if err == nil {
			if len(msg.Value) > 0 {
				kafkaMessages := make(map[string]string)
				value := make([]string, 0)
				err = MsgPackUnMarshal(msg.Value, &value)
				if err != nil {
					Logger.Println("[KAFKA ERROR] Wrong message with err:", err, "Message is:", string(msg.Value))
					continue
				}
				for i := 1; i < len(value); i += 2 {
					kafkaMessages[value[i-1]] = value[i]
				}
				message := new(ConsumerMessage)
				message.Uuid = uuid.New().String()
				message.Messages = kafkaMessages
				Logger.Println("[KAFKA]", message.Uuid, "The message is:", message.Messages)
				ConsumerMessagePipe <- *message
			}
		} else {
			// The client will automatically try to recover from all errors.
			Logger.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

func MsgPackMarshal(v interface{}) ([]byte, error) {
	var buf = new(bytes.Buffer)
	enc := codec.NewEncoder(buf, new(codec.MsgpackHandle))
	err := enc.Encode(v)
	return buf.Bytes(), err
}

func MsgPackUnMarshal(data []byte, v interface{}) error {
	var buf = bytes.NewBuffer(data)
	dec := codec.NewDecoder(buf, new(codec.MsgpackHandle))
	return dec.Decode(v)
}
