package server

import (

	//"sync"

	"context"
	"fmt"

	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/server_operations"

	"Wx_MQ/zookeeper"

	"github.com/cloudwego/kitex/server"
)

type RPCServer struct {
	srv_cli server.Server
	srv_bro server.Server
	////下面这两个分别对应上面那两个
	server   *Server
	zkServer *ZKServer
	zkInfo   zookeeper.ZKInfo
}

func NewRpcServer(zk_info zookeeper.ZKInfo) RPCServer {
	LOGinit()
	return RPCServer{
		zkInfo: zk_info,
	}
}

const (
	BROKER   = "broker"
	ZKBROKER = "zkbroker"
)

func (s *RPCServer) Start(opts_cli, opts_bro []server.Option, opt Options) error {
	// s.srv = server_operations.NewServer(s, opts...) //要使用这个函数，需要实现这个函数第一个参数，他是一个接口，那么*RPCServer就要是先这个接口下面的所有函数
	// s.server.make()
	// //go func() {
	// err := s.srv.Run()
	// DEBUG(dLOG, "broker start rpcserver")
	// if err != nil {
	// 	fmt.Println(err.Error())
	// }
	// //}()
	// return nil
	//------------------------------
	// addr, _ := net.ResolveTCPAddr("tcp", con.port)
	// var opts []server.Option
	// opts = append(opts, server.WithServiceAddr(addr))
	// //第一个参数要实现api.Client_Operations这个接口
	// con.srv = client_operations.NewServer(new(Consumer), opts...)
	switch opt.Tag {
	case BROKER:
		s.server = NewServer(s.zkInfo)
		s.server.make(opt)
	case ZKBROKER:
		s.zkServer = NewZKServer(s.zkInfo)
	}
	s.srv_bro = server_operations.NewServer(s, opts_bro...)
	go func() {
		err := s.srv_bro.Run()
		DEBUG(dLOG, "broker start rpcserver")
		if err != nil {
			fmt.Println(err.Error())
		}
	}()
	s.srv_cli = server_operations.NewServer(s, opts_cli...)
	go func() {
		err := s.srv_cli.Run()
		DEBUG(dLOG, "broker start rpcserver")
		if err != nil {
			fmt.Println(err.Error())
		}
	}()
	return nil
}
func (s *RPCServer) Stop() {
	s.srv_bro.Stop()
	s.srv_cli.Stop()
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
	err := s.server.SubHandle(SubRequest{
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

type UnknowName struct {
	TopicName     string
	PartitionName string
}

func (s *RPCServer) CreateTopic(ctx context.Context, req *api.CreateTopicRequest) (r *api.CreateTopicResponse, err error) {
	resp, err := s.zkServer.CreateTopic(UnknowName{
		TopicName: req.TopicName,
	})

}
