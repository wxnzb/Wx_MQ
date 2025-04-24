package server

import (
	"Wx_MQ/kitex_gen/api/server_operations"

	"github.com/cloudwego/kitex/server"
)

type RPCServer struct {
	server Server
}

func (s *RPCServer) start(opts []server.Option) error {
	srv := server_operations.NewServer(s, opts...)

}
