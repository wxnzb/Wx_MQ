package main

import (
	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/client_operations"
	"Wx_MQ/kitex_gen/api/server_operations"
	"context"
	"net"

	"fmt"

	"time"

	cl "github.com/cloudwego/kitex/client"
	server "github.com/cloudwego/kitex/server"
)

type Server struct {
}

// 感觉下面这个暂时没用上,我现在也不太清楚这个是干啥的
func (s *Server) Pub(ctx context.Context, req *api.PubRequest) (r *api.PubResponse, err error) {
	fmt.Println(req.Msg)
	return &api.PubResponse{
		Ret: true,
	}, nil
}
func (s *Server) Pingpong(ctx context.Context, req *api.PingpongRequest) (r *api.PingpongResponse, err error) {
	return &api.PingpongResponse{
		Pong: true,
	}, nil
}
func start_server(port string) {
	//返回的是*TCPAddr,他实现了Network函数和String函数，也就是实现了net.Addr接口
	addr, _ := net.ResolveTCPAddr("tcp", port)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))
	//第一个参数要实现api.Client_Operations这个接口
	srv := client_operations.NewServer(new(Server), opts...)
	//他返回的是一个Server接口，长这样
	// type Server interface {
	// 	RegisterService(svcInfo *serviceinfo.ServiceInfo, handler interface{}, opts ...RegisterOption) error
	// 	GetServiceInfos() map[string]*serviceinfo.ServiceInfo
	// 	Run() error
	// 	Stop() error
	// }
	err := srv.Run()
	if err != nil {
		println(err.Error())
	}
}
func main() {
	//pub and pingpong
	go start_server(":8889")
	//push pull info
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
