package main

import (
	cl2 "Wx_MQ/client/clients"
	// api "Wx_MQ/kitex_gen/api"
	// "Wx_MQ/kitex_gen/api/server_operations"
	// "context"
	// "fmt"
	"os"
	"time"
	//cl "github.com/cloudwego/kitex/client"
)

func main() {
	//这里可以进行选择是生产者还是消费者
	option := os.Args[1]
	port := ""
	if len(os.Args) == 3 {
		port = os.Args[2]
	} else {
		port = "null"
	}
	switch option {
	case "p":
		pro := cl2.NewProducer("0.0.0.0:5721", "producer-wx")
		msg := cl2.Message{
			Topic_Name:     "name",
			Partition_Name: "wuxi",
			Msg:            "like playing",
		}
		for {
			pro.Push(msg)
			time.Sleep(10 * time.Second)
		}
	case "c":
		con := cl2.NewConsumer("0.0.0.0:5721", "consumer-wx", port)
		go con.Start_server()
		con.SubScription("name", "wuxi", 0)
		con.StartGet(cl2.InfoReq{
			Topic:     "name",
			Partition: "wuxi",
			Offset:    5,
			Option:    0,
		})
	}
}
