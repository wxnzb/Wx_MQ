package server

import (
	"Wx_MQ/kitex_gen/api/server_operations"

	"sync"

	"github.com/cloudwego/kitex/server"
)

type RPCServer struct {
	logging struct {
		logger      Logger
		debug       int32 //是否开启调试日志
		trace       int32 //是否开启trace日志
		traceSysAcc int32 //trace system account是否开启系统用户的trace日志
		sync.RWMutex
	}
	server Server
}

func (s *RPCServer) start(opts []server.Option) error {
	srv := server_operations.NewServer(s, opts...)

}
