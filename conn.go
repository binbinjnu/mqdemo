package rabbitdemo

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"strconv"
	"time"
)

var (
	conn     *amqp.Connection
	mqChan   chan interface{}
	queueMap map[int]chan interface{}
)
var (
	defaultUri = "amqp://admin:123456@192.168.146.128:5672"
	exchange   = "project"
)

// NewConn 开启rabbitmq连接
func NewConn(uri string, queueNum int) {
	mqChan = make(chan interface{})
	go runConn(uri, queueNum)
}

// SendMessage 发送消息给对应协程
func SendMessage(content interface{}) {
	fmt.Println("send msg:", content)
	mqChan <- content
}

func runConn(uri string, queueNum int) {
	rand.Seed(time.Now().UnixNano())
	dail, err := amqp.Dial(uri)
	if err != nil {
		// todo 连接错误, 也要正常开起来, 需要重连
		log.Println("Failed to connect to RabbitMQ:", err.Error())
		return
	}
	conn = dail
	defer conn.Close()
	queueMap = make(map[int]chan interface{})

	// 建立n个channel并绑定
	for i := 0; i < queueNum; i++ {
		queueChan := make(chan interface{})
		go runQueue(i, queueChan)
		queueMap[i] = queueChan
	}
	for {
		timer := time.NewTimer(10 * time.Second)
		select {
		case content := <-mqChan:
			fmt.Println("in runConn, content:", content)
			// todo 负载均衡策略 随机
			number := rand.Intn(queueNum)
			// 无论connect有没有起来,
			queueMap[number] <- content
		case <-timer.C:
			fmt.Println("receive timer")
			// todo 定时处理一些事情
		}
	}

}

// StopConn 关闭rabbitmq连接
func StopConn() {
	// 每个channel关闭

	// 关闭connect
	conn.Close()

}

func runQueue(number int, c chan interface{}) error {
	// 创建一个有缓冲通道, 自己给自己发消息
	selfChan := make(chan bool, 10)
	isQueueOpen := false
	routeKey := "route_key_" + strconv.Itoa(number)
	var channel *amqp.Channel = nil
	selfChan <- isQueueOpen
	for {
		select {
		case <-selfChan:
			if !isQueueOpen {
				qChannel, err := openQueue(number)
				if err != nil {
					//
					fmt.Println("failed to open queue")
				}
				channel = qChannel
				isQueueOpen = true
			}
		case content := <-c:
			fmt.Println("in runQueue content:", content)
			// 发送message
			messageBody, _ := json.Marshal(content)
			if isQueueOpen {
				err := channel.Publish(
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
			} else {
				// todo 本地写
			}
		}
	}
	return nil
}

func openQueue(number int) (*amqp.Channel, error) {
	queue := "queue_" + strconv.Itoa(number)
	routeKey := "route_key_" + strconv.Itoa(number)
	// 创建一个channel
	// todo channel如果挂了, 需要怎么处理
	if conn == nil {
		// conn未初始化成功
		return nil, errors.New("conn nil")
	}
	channel, err := conn.Channel()
	if err != nil {
		// todo 重启queue
		log.Println("Failed to open a channel:", err.Error())
		return nil, err
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
		return nil, err
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
		return nil, err
	}
	// exchange绑定queue
	err = channel.QueueBind(queue, routeKey, exchange, false, nil)
	if err != nil {
		log.Println("Failed to bind a queue:", err.Error())
		return nil, err
	}
	return channel, nil
}
