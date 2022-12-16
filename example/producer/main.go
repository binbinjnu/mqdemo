package main

import (
	"fmt"
	"os"
	"os/signal"
	"rabbitdemo"
)

func main() {
	uri := "amqp://admin:123456@192.168.146.128:5672"
	rabbitdemo.NewConn(uri, 10)
	defer rabbitdemo.StopConn()
	for i := 0; i < 100; i++ {
		v := map[string]int{"i": i}
		rabbitdemo.SendMessage(v)
	}
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, os.Kill)
	s := <-c
	fmt.Println("Got signal:", s)
}
