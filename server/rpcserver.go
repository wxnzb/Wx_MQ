package server

import (
	"Wx_MQ/kitex_gen/api/server_operations"

	"sync"

	"context"
	"fmt"

	api "Wx_MQ/kitex_gen/api"

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

// 我终于知道为什么8889连接8888这个broker的时候总是连接不上了，因为虽然他start想通过携程启动但是mian函数已经结束了，所以就没启动,去掉go携程就好
func (s *RPCServer) Start(opts []server.Option) error {
	srv := server_operations.NewServer(s, opts...) //要使用这个函数，需要实现这个函数第一个参数，他是一个接口，那么*RPCServer就要是先这个接口下面的所有函数
	s.server.make()
	// go func() {
	err := srv.Run()
	if err != nil {
		fmt.Println(err.Error())
	}
	// }()
	return nil
}
func (s *RPCServer) Push(ctx context.Context, req *api.PushRequest) (r *api.PushResponse, err error) {
	//打印要推送的消息
	fmt.Println(req)
	err = s.server.PushHandle(Push{
		producerId: req.ProducerId,
		topic:      req.Topic,
		key:        req.Key,
		message:    req.Message,
	})
	if err == nil {
		return &api.PushResponse{
			Ret: true,
		}, nil
	}
	return &api.PushResponse{
		Ret: false,
	}, err
}

func (s *RPCServer) Pull(ctx context.Context, req *api.PullRequest) (r *api.PullResponse, err error) {
	//打印要拉取的topic和分区
	fmt.Println(req)
	//打印拉取到的消息
	ret, err := s.server.PullHandle(PullRequest{
		consumerId: req.ConsumerId,
		topic:      req.Topic,
		key:        req.Key,
	})
	if err == nil {
		return &api.PullResponse{
			Message: ret.message,
		}, nil
	}
	return &api.PullResponse{
		Message: "error",
	}, err
}

func (s *RPCServer) Info(ctx context.Context, req *api.InfoRequest) (r *api.InfoResponse, err error) {
	err = s.server.InfoHandle(req.IpPort)
	if err != nil {
		return &api.InfoResponse{
			Ret: false,
		}, err
	}
	return &api.InfoResponse{
		Ret: true,
	}, nil
}
func (s *RPCServer) Sub(ctx context.Context, req *api.SubRequest) (*api.SubResponse, error) {
	err := s.server.SubHandle(Sub{
		consumer: req.Consumer,
		topic:    req.Topic,
		key:      req.Key,
		option:   req.Option,
	})
	if err == nil {
		return &api.SubResponse{
			Ret: true,
		}, nil
	}
	return &api.SubResponse{
		Ret: false,
	}, err
}
func (s *RPCServer) StarttoGet(ctx context.Context, req *api.InfoGetRequest) (r *api.InfoGetRequest, err error) {

}
