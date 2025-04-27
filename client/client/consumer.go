package client

import (
	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/client_operations"
	"context"
	"fmt"
	"net"

	server "github.com/cloudwego/kitex/server"
)

type Consumer struct {
	cli client_operations.Client
}

func (c *Consumer) Pub(ctx context.Context, message *api.PubRequest) (r *api.PubResponse, err error) {
	fmt.Println(message)
	return &api.PubResponse{
		Ret: true,
	}, nil
}
func (c *Consumer) Pingpong(ctx context.Context, req *api.PingpongRequest) (r *api.PingpongResponse, err error) {
	return &api.PingpongResponse{
		Pong: true,
	}, nil
}
func start_server(port string) {
	addr, _ := net.ResolveTCPAddr("tcp", port)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))
	srv := client_operations.NewServer(new(Consumer), opts...)
	err := srv.Run()
	if err != nil {
		println(err.Error())
	}
}
