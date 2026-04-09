package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

type Acktype int

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}
	ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	})
	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	dur, autoD, exc := false, false, false

	if queueType == Durable {
		dur = true
	} else {
		autoD = true
		exc = true
	}

	queue, err := ch.QueueDeclare(queueName, dur, autoD, exc, false, amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	})
	ch.QueueBind(queueName, key, exchange, false, nil)
	return ch, queue, nil
}

// func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) Acktype) error {
// 	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
// 	if err != nil {
// 		return err
// 	}
// 	chDelivery, err := ch.Consume(queueName, "", false, false, false, false, nil)
// 	if err != nil {
// 		return err
// 	}
//
// 	go func() {
// 		for me := range chDelivery {
// 			var del T
// 			json.Unmarshal(me.Body, &del)
// 			fb := handler(del)
// 			if fb == Ack {
// 				fmt.Println("Acktype: Ack")
// 				err = me.Ack(false)
// 			}
// 			if fb == NackDiscard {
// 				fmt.Println("Acktype: NackDiscard")
// 				err = me.Nack(false, false)
// 			}
// 			if fb == NackRequeue {
// 				fmt.Println("Acktype: NackRequeue")
// 				err = me.Nack(false, true)
// 			}
// 		}
// 	}()
// 	return nil
// }

func UnmarshalJSON[T any](data []byte) (T, error) {
	var del T
	err := json.Unmarshal(data, &del)
	if err != nil {
		return del, err
	}
	return del, nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var net bytes.Buffer
	enc := gob.NewEncoder(&net)
	err := enc.Encode(val)

	if err != nil {
		return err
	}

	ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        net.Bytes(),
	})
	return nil
}

func PubGLog(ch *amqp.Channel, username, message string) error {
	key := routing.GameLogSlug + "." + username
	data := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     message,
		Username:    username,
	}
	return PublishGob(ch, routing.ExchangePerilTopic, key, data)
}

// func SubscribeGob[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) Acktype) error {
// 	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
// 	if err != nil {
// 		return err
// 	}
// 	chDelivery, err := ch.Consume(queueName, "", false, false, false, false, nil)
// 	if err != nil {
// 		return err
// 	}
//
// 	go func() {
// 		for me := range chDelivery {
// 			var del T
// 			net := bytes.NewBuffer(me.Body)
// 			dec := gob.NewDecoder(net)
// 			err := dec.Decode(&del)
//
// 			if err != nil {
// 				log.Fatalf("Failed decoding message")
// 			}
//
// 			fb := handler(del)
// 			if fb == Ack {
// 				fmt.Println("Acktype: Ack")
// 				err = me.Ack(false)
// 			}
// 			if fb == NackDiscard {
// 				fmt.Println("Acktype: NackDiscard")
// 				err = me.Nack(false, false)
// 			}
// 			if fb == NackRequeue {
// 				fmt.Println("Acktype: NackRequeue")
// 				err = me.Nack(false, true)
// 			}
// 		}
// 	}()
// 	return nil
// }

func UnmarshalGob[T any](data []byte) (T, error) {
	var del T
	net := bytes.NewBuffer(data)
	dec := gob.NewDecoder(net)
	err := dec.Decode(&del)
	if err != nil {
		return del, err
	}
	return del, nil
}

func Subscribe[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) Acktype, unmarshaller func([]byte) (T, error)) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	chDelivery, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for me := range chDelivery {
			del, err := unmarshaller(me.Body)
			if err != nil {
				log.Fatalf("Error unmarshalling data")
			}
			fb := handler(del)
			if fb == Ack {
				fmt.Println("Acktype: Ack")
				err = me.Ack(false)
			}
			if fb == NackDiscard {
				fmt.Println("Acktype: NackDiscard")
				err = me.Nack(false, false)
			}
			if fb == NackRequeue {
				fmt.Println("Acktype: NackRequeue")
				err = me.Nack(false, true)
			}

		}
	}()
	return nil
}
