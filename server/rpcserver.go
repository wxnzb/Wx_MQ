package server

import (
	"Wx_MQ/kitex_gen/api/server_operations"
	"encoding/json"

	//"sync"

	"context"
	"fmt"

	api "Wx_MQ/kitex_gen/api"

	"github.com/cloudwego/kitex/server"
)

type RPCServer struct {
	srv    server.Server
	server *Server
}

func NewRpcServer() RPCServer {
	LOGinit()
	return RPCServer{
		server: NewServer(),
	}
}

func (s *RPCServer) Start(opts []server.Option) error {
	s.srv = server_operations.NewServer(s, opts...) //要使用这个函数，需要实现这个函数第一个参数，他是一个接口，那么*RPCServer就要是先这个接口下面的所有函数
	s.server.make()
	//go func() {
	err := s.srv.Run()
	DEBUG(dLOG, "broker start rpcserver")
	if err != nil {
		fmt.Println(err.Error())
	}
	//}()
	return nil
}
func (s *RPCServer) Stop() {
	s.srv.Stop()
}

// 生产者	向 Broker 投递一条消息
func (s *RPCServer) Push(ctx context.Context, req *api.PushRequest) (r *api.PushResponse, err error) {
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

// 消费者	从 Broker 拉取消息（主动消费）
func (s *RPCServer) Pull(ctx context.Context, req *api.PullRequest) (r *api.PullResponse, err error) {
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

// 消费者	向 Broker 注册自己的 IP:Port（上线注册）
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

// 消费者	订阅某个 topic 的数据
func (s *RPCServer) Sub(ctx context.Context, req *api.SubRequest) (*api.SubResponse, error) {
	res, err := s.server.SubHandle(SubRequest{
		consumer: req.Consumer,
		topic:    req.Topic,
		key:      req.Key,
		option:   req.Option,
	})
	partsName_data, _ := json.Marshal(res.parts)
	if err == nil {
		return &api.SubResponse{
			Ret:   true,
			Size:  int64(res.size),
			Parts: partsName_data,
		}, nil
	}
	return &api.SubResponse{
		Ret: false,
	}, err
}

// 消费者	指示开始消费某个分区的消息，从某个 offset 开始
func (s *RPCServer) StarttoGet(ctx context.Context, req *api.InfoGetRequest) (r *api.InfoGetResponse, err error) {

	err = s.server.StartGet(PartitionInitInfo{
		topic:           req.Topic_Name,
		partition:       req.Partition_Name,
		consumer_ipname: req.Cli_Name,
		index:           req.Offset,
	})
	if err != nil {
		return &api.InfoGetResponse{Ret: false}, err
	}
	return &api.InfoGetResponse{Ret: true}, nil

}
