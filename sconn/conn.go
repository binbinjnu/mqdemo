package sconn

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

type Session struct {
	name            string
	logger          *log.Logger
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
}

var (
	SESSION *Session
)

const (
	// connection的重连时长
	reconnectDelay = 5 * time.Second

	// channel exception后重新init的时长
	reInitDelay = 2 * time.Second

	// message 没有收到confirm时重发的时长
	resendDelay = 5 * time.Second
)

var (
	errNotConnected  = errors.New("not connected to a server")
	errAlreadyClosed = errors.New("already close: not connected to the server")
	errShutdown      = errors.New("session is shutting down")
)

//func New(name string, addr string) {
//	SESSION = NewSession(name, addr)
//}

//
func New(name string, addr string) *Session {
	session := &Session{
		logger: log.New(os.Stdout, "", log.LstdFlags),
		name:   name,
		done:   make(chan bool),
	}
	go session.handleReconnect(addr)
	return session
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (session *Session) handleReconnect(addr string) {
	for {
		session.isReady = false
		log.Println("Attempting to connect")
		conn, err := session.connect(addr)
		if err != nil {
			log.Println("Failed to connect. Retrying...")
			select {
			case <-session.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}
		if done := session.handleReInit(conn); done {
			break
		}
	}
}

// connect will create a new AMQP connection
// 建立新的AMQP连接
func (session *Session) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, err
	}
	session.changeConnection(conn)
	log.Println("Connected!")
	return conn, nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
// 等待一个channel error, 然后尝试重新初始化channel
func (session *Session) handleReInit(conn *amqp.Connection) bool {
	for {
		session.isReady = false
		err := session.init(conn)

		if err != nil {
			log.Println("Failed to init channel. Retrying...")
			select {
			case <-session.done:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}
		select {
		case <-session.done:
			return true
		case <-session.notifyConnClose:
			log.Println("Connection closed. Reconnecting...")
			return false
		case <-session.notifyChanClose:
			log.Println("Channel closed. Rerunning init...")
		}
	}
}

// init will initialize channel & declare queue
// 初始化channel和声明queue
func (session *Session) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	err = ch.Confirm(false)
	if err != nil {
		return err
	}
	_, err = ch.QueueDeclare(
		session.name,
		false, // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		return err
	}
	session.changeChannel(ch)
	session.isReady = true
	log.Println("Setup")
	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
// 更新session.connection
func (session *Session) changeConnection(conn *amqp.Connection) {
	session.connection = conn
	session.notifyConnClose = make(chan *amqp.Error)
	session.connection.NotifyClose(session.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
// changeChannel 更新channel
func (session *Session) changeChannel(channel *amqp.Channel) {
	session.channel = channel
	session.notifyChanClose = make(chan *amqp.Error)
	session.notifyConfirm = make(chan amqp.Confirmation, 1)
	session.channel.NotifyClose(session.notifyChanClose)
	session.channel.NotifyPublish(session.notifyConfirm)
}

// Push will push data onto the queue, and wait for a confirm.
// If no confirms are received until within the resendTimeout,
// it continuously re-sends messages until a confirm is received.
// This will block until the server sends a confirm. Errors are
// only returned if the push action itself fails, see UnsafePush.
func (session *Session) Push(data []byte) error {
	if !session.isReady {
		return errors.New("failed to push: not connected")
	}
	for {
		err := session.UnsafePush(data)
		if err != nil {
			session.logger.Println("Push failed. Retrying...")
			select {
			case <-session.done:
				return errShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		select {
		case confirm := <-session.notifyConfirm:
			if confirm.Ack {
				session.logger.Println("Push confirmed!")
				return nil
			}
		case <-time.After(resendDelay):
		}
		session.logger.Println("Push didn't confirm. Retrying...")
	}
	return nil
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// recieve the message.
func (session *Session) UnsafePush(data []byte) error {
	if !session.isReady {
		return errNotConnected
	}
	return session.channel.Publish(
		"",           // Exchange
		session.name, // Routing key
		false,        // Mandatory
		false,        // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		},
	)
}

// Stream will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (session *Session) Stream() (<-chan amqp.Delivery, error) {
	if !session.isReady {
		return nil, errNotConnected
	}
	return session.channel.Consume(
		session.name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

}

// Close 关闭channel和conn
// Close will cleanly shutdown the channel and connection.
func (session *Session) Close() error {
	if !session.isReady {
		return errAlreadyClosed
	}
	err := session.channel.Close()
	if err != nil {
		return err
	}
	err = session.connection.Close()
	if err != nil {
		return err
	}
	close(session.done)
	session.isReady = false
	return nil
}
