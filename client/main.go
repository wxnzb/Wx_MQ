package main

import (
	api "Wx_MQ/kitex_gen/api"
	//"Wx_MQ/kitex_gen/api/client_operations"
	"Wx_MQ/kitex_gen/api/server_operations"
	"context"

	//"net"

	"fmt"

	"time"

	cl "github.com/cloudwego/kitex/client"
	//server "github.com/cloudwego/kitex/server"
	cl2 "Wx_MQ/client/client"
)

func main() {
	// 	//pub and pingpong
	// 	go Start_server(":8889")
	// 	//push pull info
	client, err := server_operations.NewClient("client", cl.WithHostPorts("0.0.0.0:8888"))
	//他返回的是Client接口，长这样
	// type Client interface {
	// 	Push(ctx context.Context, req *api.PushRequest, callOptions ...callopt.Option) (r *api.PushResponse, err error)
	// 	Pull(ctx context.Context, req *api.PullRequest, callOptions ...callopt.Option) (r *api.PullResponse, err error)
	// 	Info(ctx context.Context, req *api.InfoRequest, callOptions ...callopt.Option) (r *api.InfoResponse, err error)
	// }
	if err != nil {
		fmt.Println(err)
	}
	info := &api.InfoRequest{
		IpPort: "0.0.0.0:8889",
	}
	consumer := cl2.Consumer{}
	consumer.Cli = client
	//这个是为了反向代理，当broker有消息要发给这个中转站的时候就能找到了
	resp, err := client.Info(context.Background(), info)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(resp)
	}
	for {
		req := &api.PushRequest{
			ProducerId: 1,
			Topic:      "name",
			Key:        "wuxi",
			Message:    "like playing",
		}
		resp, err := client.Push(context.Background(), req)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(resp)
		time.Sleep(10 * time.Second)
	}
}
