package main

import (
	cl2 "Wx_MQ/client/clients"
	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/server_operations"
	"context"
	"fmt"
	"os"
	"time"

	cl "github.com/cloudwego/kitex/client"
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
	//  Sub(ctx context.Context, req *api.SubRequest, callOptions ...callopt.Option) (r *api.SubResponse, err error)
	// }
	if err != nil {
		fmt.Println(err)
	}
	//这里可以进行选择是生产者还是消费者
	option := os.Args[1]
	port := ""
	if len(os.Args) == 3 {
		port = os.Args[2]
	} else {
		port = "null"
	}
	ipport := ""
	switch option {
	case "p":
		pro := cl2.Producer{}
		pro.Cli = client
		//这个还没有实现呢
		pro.Name = cl2.GetIpPort() + port
		ipport = pro.Name
	case "c":
		con := cl2.Consumer{}
		con.Cli = client
		con.Name = cl2.GetIpPort() + port
		ipport = con.Name
		//这里为什么要加:??开启这个消费者客户端的rpc服务器
		go con.Start_server(":" + port)
	}
	info := &api.InfoRequest{
		IpPort: ipport,
	}
	//这个是为了反向代理，当broker有消息要发给这个中转站的时候就能找到了
	//这里一般是c的时候能用上，要让broker知道这个消费者的ip和端口
	resp, err := client.Info(context.Background(), info)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(resp)
	}
	for {
		req := &api.PushRequest{
			//这里也是，现在他是string,应该是int64
			ProducerId: ipport,
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
