package client

import (
	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/client_operations"
	"context"
	"fmt"
	"net"

	"Wx_MQ/kitex_gen/api/server_operations"

	server "github.com/cloudwego/kitex/server"
)

type Consumer struct {
	Cli server_operations.Client
}

//	func (con *Consumer) Pub(ctx context.Context, req *api.PubRequest) (r *api.PubResponse, err error) {
//		fmt.Println(req.Msg)
//		return &api.PubResponse{
//			Ret: true,
//		}, nil
//	}
//
//	func (con *Consumer) Pingpong(ctx context.Context, req *api.PingpongRequest) (r *api.PingpongResponse, err error) {
//		return &api.PingpongResponse{
//			Pong: true,
//		}, nil
//	}
//
//	func Start_server(port string) {
//		addr, _ := net.ResolveTCPAddr("tcp", port)
//		var opts []server.Option
//		opts = append(opts, server.WithServiceAddr(addr))
//		srv := client_operations.NewServer(new(Consumer), opts...)
//		err := srv.Run()
//		if err != nil {
//			println(err.Error())
//		}
//	}
//
// 下面这个是启动一个rpc服务器，broker服务器测试消息推送功能或探活功能就是通过和他交流的
func (con *Consumer) Pub(ctx context.Context, req *api.PubRequest) (r *api.PubResponse, err error) {
	fmt.Println(req.Msg)
	return &api.PubResponse{
		Ret: true,
	}, nil
}

func (con *Consumer) Pingpong(ctx context.Context, req *api.PingpongRequest) (r *api.PingpongResponse, err error) {
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
	srv := client_operations.NewServer(new(Consumer), opts...)
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
