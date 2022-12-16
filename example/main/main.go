package main

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
)

var (
	conn *amqp.Connection
)
var (
	uri      = "amqp://admin:123456@192.168.146.128:5672"
	exchange = "project"
)
var queueInfo = map[string][2]string{
	"log1": {"queue1", "routeKey1"},
	"log2": {"queue2", "routeKey2"},
}

func main() {
	// 建立连接
	dail, err := amqp.Dial(uri)
	conn = dail
	if err != nil {
		log.Println("Failed to connect to RabbitMQ:", err.Error())
		return
	}
	defer conn.Close()
	c1 := make(chan interface{})
	c2 := make(chan interface{})
	go queue("queue1", "routeKey1", c1)
	go queue("queue2", "routeKey2", c2)
	c1 <- map[string]interface{}{"c1": "11111"}
	c2 <- map[string]interface{}{"c2": "22222"}
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, os.Kill)
	s := <-c
	fmt.Println("Got signal:", s)
}

func queue(queue, routeKey string, c chan interface{}) error {
	// 创建一个channel
	channel, err := conn.Channel()
	defer channel.Close()
	if err != nil {
		log.Println("Failed to open a channel:", err.Error())
		return err
	}
	// 声明exchange
	err = channel.ExchangeDeclare(
		exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Println("Failed to declare a exchange:", err.Error())
		return err
	}

	// 声明一个queue
	_, err = channel.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Println("Failed to declare a queue:", err.Error())
		return err
	}
	// exchange绑定queue
	channel.QueueBind(queue, routeKey, exchange, false, nil)
	for {
		content := <-c
		fmt.Println(content)
		// 发送message
		messageBody, _ := json.Marshal(content)
		err = channel.Publish(
			exchange,
			routeKey,
			false,
			false,
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "text/plain",
				ContentEncoding: "",
				Body:            messageBody,
			},
		)
		if err != nil {
			log.Println("Failed to publish a message:", err.Error())
			// todo 本地写
		}
	}

	return nil
}

func pubMq(uri, exchange, queue, routeKey string, content map[string]interface{}) error {
	// 建立连接
	conn, err := amqp.Dial(uri)
	if err != nil {
		log.Println("Failed to connect to RabbitMQ:", err.Error())
		return err
	}
	// 创建一个channel
	channel, err := conn.Channel()
	if err != nil {
		log.Println("Failed to open a channel:", err.Error())
		return err
	}
	// 声明exchange
	err = channel.ExchangeDeclare(
		exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Println("Failed to declare a exchange:", err.Error())
		return err
	}
	// 声明一个queue
	_, err = channel.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Println("Failed to declare a queue:", err.Error())
		return err
	}
	// exchange绑定queue
	channel.QueueBind(queue, routeKey, exchange, false, nil)

	// 发送message
	messageBody, _ := json.Marshal(content)
	err = channel.Publish(
		exchange,
		routeKey,
		false,
		false,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            messageBody,
		},
	)
	if err != nil {
		log.Println("Failed to publish a message:", err.Error())
		return err
	}
	return nil
}
