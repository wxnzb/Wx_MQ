package client

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
	go start_server(":8888")
	client, err := server_operations.NewClient("client", cl.WithHostPorts("0.0.0.0:8888"))
	//他返回的是Client接口，长这样
	// type Client interface {
	// 	Push(ctx context.Context, req *api.PushRequest, callOptions ...callopt.Option) (r *api.PushResponse, err error)
	// 	Pull(ctx context.Context, req *api.PullRequest, callOptions ...callopt.Option) (r *api.PullResponse, err error)
	// 	Info(ctx context.Context, req *api.InfoRequest, callOptions ...callopt.Option) (r *api.InfoResponse, err error)
	// }
	if err != nil {
		println(err.Error())
	}
	info := &api.InfoRequest{
		IpPort: "0.0.0.0:8888",
	}
	resp, err := client.Info(context.Background(), info)
	if err != nil {
		println(resp)
	}
	for {
		req := &api.PushRequest{
			ProducerId: 1,
			Topic:      "wuxi",
			Key:        "hah",
			Message:    "kk",
		}
		resp, err := client.Push(context.Background(), req)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(resp)
		time.Sleep(10 * time.Second)
	}
}
