package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"rabbitdemo/mymq"
	"time"
)

func main() {
	//if st == nil {
	//	fmt.Println("is nil ")
	//}
	//name := "duomi_queue"
	//addr := "amqp://admin:123456@192.168.146.128:5672"
	//mq := sconn.New(name, addr)
	//message := []byte("message")
	//time.Sleep(time.Second * 3)
	//mq.Push(message)
	//c := make(chan os.Signal)
	//signal.Notify(c, os.Interrupt, os.Kill)
	//s := <-c
	//fmt.Println("Got signal:", s)

	addr := "amqp://admin:123456@192.168.146.128:5672"
	mymq.Open(addr)

	log.Println("before time sleep")
	time.Sleep(10 * time.Second)
	log.Println("after time sleep")
	mymq.Close()
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, os.Kill)
	s := <-c
	fmt.Println("Got signal:", s)

}
