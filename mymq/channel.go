package mymq

import (
	"github.com/streadway/amqp"
	"log"
	"strconv"
	"time"
)

type (
	// channel的session
	ChSession struct {
		index           int
		name            string
		channel         *amqp.Channel
		notifyChanClose chan *amqp.Error
		notifyConfirm   chan amqp.Confirmation
		done            chan bool
		isReady         bool
	}
)

const (

	// channel exception后重新init的时长
	reInitDelay = 5 * time.Second

	// 计算队列消息数时间间隔
	countDelay = 5 * time.Second
)

func NewChSession(index int) *ChSession {
	return &ChSession{
		index: index,
		name:  "dmqueue_" + strconv.Itoa(index),
		done:  make(chan bool),
	}
}

func (chS *ChSession) CloseChSession() {
	close(chS.done)
	if chS.channel != nil {
		chS.channel.Close()
	}
}

func (chS *ChSession) handleChannel(conn *amqp.Connection) {
FOR1:
	for {
		chS.isReady = false
		err := chS.init(conn)
		//err := errors.New("abc")

		if err != nil {
			log.Println("Failed to init channel. Retrying...")
			select {
			case <-chS.done:
				chS.isReady = false
				log.Println("channel done in FOR1!")
				// 在init错误的时候收到done信息, 直接关掉整个协程
				break FOR1
			case <-time.After(reInitDelay):
				log.Println("receive re init delay!")
				// init错误, 等待n秒继续init
			}
			continue FOR1
		}
	FOR2:
		for {
			select {
			case <-chS.done:
				chS.isReady = false
				log.Println("channel done in FOR2!")
				break FOR1
			case <-chS.notifyChanClose:
				// 重新跑FOR1循环
				log.Println("Channel closed. Rerunning init...")
				break FOR2
			case <-time.After(countDelay):
				// 继续handle的for循环
				queue, err := chS.channel.QueueInspect(chS.name)
				//err := errors.New("haha")
				if err != nil {
					// 继续FOR2循环
					continue FOR2
				}
				log.Println("message num is ", queue.Messages)
				// todo 计算队列消息数, 如果超过, 设标记位, 一直到水位下降
			}
		}
	}
}

// init will initialize channel & declare queue
// 初始化channel和声明queue
func (chS *ChSession) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	err = ch.Confirm(false)
	if err != nil {
		return err
	}
	_, err = ch.QueueDeclare(
		chS.name,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		return err
	}

	chS.channel = ch
	chS.notifyChanClose = make(chan *amqp.Error)
	chS.notifyConfirm = make(chan amqp.Confirmation, 1)
	chS.channel.NotifyClose(chS.notifyChanClose)
	chS.channel.NotifyPublish(chS.notifyConfirm)

	chS.isReady = true
	log.Println("Setup")
	return nil
}
