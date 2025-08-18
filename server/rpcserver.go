package server

import (

	//"sync"

	"context"
	"encoding/json"
	"fmt"
	"io"

	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/server_operations"

	"Wx_MQ/zookeeper"

	"Wx_MQ/kitex_gen/api/zkserver_operations"

	"github.com/cloudwego/kitex/server"
)

type RPCServer struct {
	name string
	//这里用指针更好？？？
	srv_cli *server.Server
	srv_bro *server.Server
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
	switch opt.Tag {
	//初始化一个broker的server结构体，他在注册的时候就要向zk注册
	case BROKER:
		s.server = NewServer(s.zkInfo)
		s.server.make(opt)
		//面向客户端的服务端
		srv_cli := server_operations.NewServer(s, opts_cli...)
		s.srv_cli = &srv_cli
		go func() {
			err := srv_cli.Run()
			//DEBUG(dLOG, "broker start rpcserver")
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
		//初始化一个zk的server结构体
	case ZKBROKER:
		s.zkServer = NewZKServer(s.zkInfo)
		s.zkServer.make(opt)
		//面向broker的服务端
		srv_bro := zkserver_operations.NewServer(s, opts_bro...)
		s.srv_bro = &srv_bro
		go func() {
			err := srv_bro.Run()
			//DEBUG(dLOG, "broker start rpcserver")
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}
	return nil
}
func (s *RPCServer) Stop() {
	if s.srv_bro != nil {
		(*s.srv_bro).Stop()
	}
	if s.srv_cli != nil {
		(*s.srv_cli).Stop()
	}
}

//	service Server_Operations{
//	    //producer
//	    PushResponse push(1:PushRequest req)
//	    //consumer
//	    PullResponse pull(1:PullRequest req)
//	    infoResponse info(1:infoRequest req)
//	    InfoGetResponse StarttoGet(1:InfoGetRequest req)
//	    //上面这些是消费者和客户端根broker交流，下面的是broker和broker之间的交流
//	    //1:通知目标 Broker：准备接收某文件
//	    PrepareAcceptResponse prepareAccept(1:PrepareAcceptRequest req)
//	    //2:通知接收方“我要从 offset 开始，发送某个文件的某部分了”，请确认你准备好了，或者已经收到了这部分
//	    PrepareSendResponse prepareSend(1:PrepareSendRequest req)
//	}
//
// 生产者	向 Broker 投递一条消息
func (s *RPCServer) Push(ctx context.Context, req *api.PushRequest) (r *api.PushResponse, err error) {
	fmt.Println(req)
	ret, err := s.server.PushHandle(Info{
		producer: req.ProducerId,
		//-----------------
		topic:     req.Topic,
		partition: req.Key,
		//------------------
		message: req.Message,
		size:    req.Size,
		//-----------------
		//检验类型
		ack: req.Ack,
		//这个在ack==-1的时候leader同步会用到
		cmdindex: req.Comindex,
	})
	if err == nil {
		DEBUG(dError, err.Error())
	}
	return &api.PushResponse{
		Ret: false,
		Err: ret,
	}, err
}

// 消费者	从 Broker 拉取消息（主动消费）
func (s *RPCServer) Pull(ctx context.Context, req *api.PullRequest) (r *api.PullResponse, err error) {

	ret, err := s.server.PullHandle(Info{
		consumer:  req.ConsumerId,
		topic:     req.Topic,
		partition: req.Key,
		//下面这三个是消费者向消费的消息数量,消费策略还有从分区的offset开始消费
		size:   req.Size,
		option: req.Option,
		offset: req.Offset,
	})
	Err := "ok"
	if err != nil {
		if err == io.EOF && ret.size == 0 {
			Err = "file EOF"
		} else {
			DEBUG(dError, "pull err")
		}
		return &api.PullResponse{
			Ret: false,
			Err: err.Error(),
		}, err
	}
	return &api.PullResponse{
		Msgs:       ret.array,
		StartIndex: ret.start_index,
		EndIndex:   ret.end_index,
		Size:       ret.size,
		Err:        Err,
	}, nil
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

// 消费者	指示开始消费某个分区的消息，从某个 offset 开始
func (s *RPCServer) StarttoGet(ctx context.Context, req *api.InfoGetRequest) (r *api.InfoGetResponse, err error) {

	err = s.server.StartGet(Info{
		topic:           req.Topic_Name,
		partition:       req.Partition_Name,
		consumer_ipname: req.Consumer_Name,
		offset:          req.Offset,
	})
	if err != nil {
		return &api.InfoGetResponse{Ret: false}, err
	}
	return &api.InfoGetResponse{Ret: true}, nil
}

// //-----------------------新加zkserver-bro，主broker先是让他准备好接收，然后告诉从节点具体接收文件的位置
// [zkserver]             [Broker ]
//    |                        |
//    |--- prepareAccept ----->|   （通知准备同步一段数据）
//    |                        |
//    |<-- AcceptResponse -----|   （确认可以接收）
//    |                        |
//    |--- prepareSend ------->|   （发送前再次协商当前 offset）
//    |                        |
//    |<-- SendResponse -------|   （确认当前 offset、是否重复）
//    |                        |
//    |=== 数据同步流（HTTP / RPC / 其他） ===>

func (s *RPCServer) PrepareAccept(ctx context.Context, req *api.PrepareAcceptRequest) (r *api.PrepareAcceptResponse, err error) {
	errs, err := s.server.PrepareAcceptHandle(Info{
		topic:     req.Topic_Name,
		partition: req.Partition_Name,
		file_name: req.File_Name,
	})
	if err != nil {
		return &api.PrepareAcceptResponse{
			Ret: false,
			Err: errs,
		}, err
	}
	return &api.PrepareAcceptResponse{
		Ret: true,
		Err: "",
	}, nil
}

// zkserver控制broker停止接收某个partition的信息
// 修改文件名，关闭partition中的信息
// 调用者要想zookeeper修改节点
func (s *RPCServer) CloseAccept(ctx context.Context, req *api.CloseAcceptRequest) (r *api.CloseAcceptResponse, err error) {
	start, end, _, err := s.server.CloseAcceptHandle(Info{
		topic:        req.Topic_Name,
		partition:    req.Partition_Name,
		file_name:    req.OldFile_Name,
		newfile_name: req.NewFile_Name_,
	})
	if err != nil {
		return &api.CloseAcceptResponse{
			Ret: false,
		}, err
	}
	return &api.CloseAcceptResponse{
		Ret:        true,
		Startindex: start,
		Endindex:   end,
	}, nil
}
func (s *RPCServer) PrepareSend(ctx context.Context, req *api.PrepareSendRequest) (r *api.PrepareSendResponse, err error) {
	errs, err := s.server.PrepareSendHandle(Info{
		topic:     req.Topic_Name,
		partition: req.Partition_Name,
		file_name: req.File_Name,
		offset:    req.Offset,
		option:    req.Option,
	})
	if err != nil {
		return &api.PrepareSendResponse{
			Ret: false,
			Err: errs,
		}, err
	}
	return &api.PrepareSendResponse{
		Ret: true,
		Err: "",
	}, nil
}
func (s *RPCServer) PrepareState(ctx context.Context, req *api.PrepareStateRequest) (r *api.PrepareStateResponse, err error) {
	var Brokers BrokerS
	json.Unmarshal(req.Brokers, &Brokers)
	ret, err := s.server.PrepareStateHandle(Info{
		topic:     req.TopicName,
		partition: req.PartName,
		brokers:   Brokers.BroBrokers,
		option:    req.State,
	})
	if err != nil {
		return &api.PrepareStateResponse{
			Ret: false,
			Err: ret,
		}, err
	}
	return &api.PrepareStateResponse{
		Ret: true,
		Err: ret,
	}, nil
}

// raft
type BrokerS struct {
	BroBrokers  map[string]string `json:brobrokers`
	RaftBrokers map[string]string `json:rafbrokers`
	Me_Brokers  map[string]int    `json:mebrokers`
}

func (s *RPCServer) AddRaftPartition(ctx context.Context, req *api.AddRaftPartitionRequest) (r *api.AddRaftPartitionResponse, err error) {
	var Brokers BrokerS
	json.Unmarshal(req.Brokers, &Brokers)
	ret, err := s.server.AddRaftPartitionHandle(Info{
		topic:     req.TopicName,
		partition: req.PartName,
		brokers:   Brokers.RaftBrokers,
		brok_me:   Brokers.Me_Brokers,
	})
	if err != nil {
		return &api.AddRaftPartitionResponse{
			Ret: false,
			Err: ret,
		}, err
	}
	return &api.AddRaftPartitionResponse{
		Ret: true,
		Err: ret,
	}, nil
}
func (s *RPCServer) CloseRaftPartition(ctx context.Context, req *api.CloseRaftPartitionRequest) (r *api.CloseRaftPartitionResponse, err error) {
	ret, err := s.server.CloseRaftPartitionHandle(Info{
		topic:     req.TopicName,
		partition: req.PartName,
	})
	if err != nil {
		return &api.CloseRaftPartitionResponse{
			Ret: false,
			Err: ret,
		}, err
	}
	return &api.CloseRaftPartitionResponse{
		Ret: true,
		Err: ret,
	}, nil
}
func (s *Server) AddFetchPartition(ctx context.Context, req *api.AddFetchPartitionRequest) (r *api.AddFetchPartitionResponse, err error) {
	var Brokers BrokerS
	json.Unmarshal(req.Brokers, &Brokers)
	ret, err := s.server.AddFetchPartitionHandle(Info{
		topic:        req.TopicName,
		partition:    req.PartName,
		brokers:      Brokers.BroBrokers,
		LeaderBroker: req.LeaderBroker,
		HostPort:     req.HostPort,
		file_name:    req.FileName,
	})
	if err != nil {
		return &api.AddFetchPartitionResponse{
			Ret: false,
			Err: ret,
		}, err
	}
	return &api.AddFetchPartitionResponse{
		Ret: true,
		Err: ret,
	}, nil
}
func (s *Server) CloseFetchPartition(ctx context.Context, req *api.CloseFetchPartitionRequest) (r *api.CloseFetchPartitionResponse, err error) {

}

//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// service ZKServer_Operations{
//     //producer
//     ProGetBroResponse ProGetBro(1:ProGetBroRequest req)
//     ProSetPartStateResponse ProSetPart(1:ProSetPartStateRequest req)
//     //consumer
//     ConGetBroResponse ConGetBro(1:ConGetBroRequest req)
//     SubResponse sub(1:SubRequest req)
//     ConStartGetBro(ctx context.Context, req *ConStartGetBroRequest) (r *ConStartGetBroResponse, err error)
//      //broker
//     BroInfoResponse  BroInfo(1:BroInfoRequest req)
//     BroGetAssignResponse BroGetssign(1:BroGetAssignRequest req)
//     UpdateOffset(ctx context.Context, req *UpdateOffsetRequest) (r *UpdateOffsetResponse, err error)
//     //------------------
//     CreateTopicResponse CreateTopic(1:CreateTopicRequest req)
//     CreatePartitionResponse CreatePartition(1:CreatePartitionRequest req)

// }
//
//	struct ProGetBroRequest{
//	    1:string topic_name
//	    2:string partition_name
//	}
//
//	struct ProGetBroResponse{
//	    1:bool ret
//	    2:string bro_host_port
//	}
//
// //消费者想消费某个 topic 的某个 partition，于是去询问哪个 broker 负责它。
//
//	struct ConGetBroRequest{
//	    1:string topic_name
//	    2:string partition_name
//	    3:i8 option
//	}
//
//	struct ConGetBroResponse{
//	    1:bool ret
//	    2:i64 size
//	    3:binary bros
//	    4:binary parts
//	}
// type Info_in struct {
// 	TopicName     string
// 	PartitionName string
// 	Option        int8
// }
// type Info_out struct {
// 	Err error
// }

func (s *RPCServer) ProGetBro(ctx context.Context, req *api.ProGetBroRequest) (r *api.ProGetBroResponse, err error) {
	info_out := s.zkServer.ProGetBroHandle(Info_in{
		TopicName:     req.TopicName,
		PartitionName: req.PartitionName,
	})
	if info_out.Err != nil {
		return &api.ProGetBroResponse{
			Ret: false,
		}, info_out.Err
	}
	return &api.ProGetBroResponse{
		Ret:         true,
		BroHostPort: info_out.bro_host_port,
	}, info_out.Err
}
func (s *RPCServer) ProSetPartState(ctx context.Context, req *api.ProSetPartStateRequest) (r *api.ProSetPartStateResponse, err error) {
	info_out := s.zkServer.ProSetPartStateHandle(Info_in{
		TopicName:     req.Topic,
		PartitionName: req.Partition,
		Option:        req.Option,
		Dupnum:        req.Dupnum,
	})
	if info_out.Err != nil {
		return &api.ProSetPartStateResponse{
			Ret: false,
		}, info_out.Err
	}
	return &api.ProSetPartStateResponse{
		Ret: true,
		Err: "ok",
	}, nil
}

// 生产者设置某个分区的状态
// 需要补充
func (s *RPCServer) ConGetBro(ctx context.Context, req *api.ConGetBroRequest) (r *api.ConGetBroResponse, err error) {
	parts, size, err := s.zkServer.ConGetBroHandle(Info_in{
		TopicName:     req.TopicName,
		PartitionName: req.PartitionName,
		Option:        req.Option,
		CliName:       req.CliName,
		Index:         req.Index,
	})
	if err != nil {
		return &api.ConGetBroResponse{
			Ret: false,
		}, err
	}
	return &api.ConGetBroResponse{
		Ret: true,
		//为啥需要下面这两个
		Size:  int64(size),
		Parts: parts,
	}, nil
}

// 消费者	订阅某个 topic 的数据
func (s *RPCServer) Sub(ctx context.Context, req *api.SubRequest) (*api.SubResponse, error) {
	err := s.zkServer.SubHandle(Info_in{
		CliName:       req.Consumer,
		TopicName:     req.Topic,
		PartitionName: req.Key,
		Option:        req.Option,
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

// broker---->zkserver
func (s *RPCServer) UpdateOffset(ctx context.Context, req *api.UpdateOffsetRequest) (r *api.UpdateOffsetResponse, err error) {
	err = s.zkServer.UpdateOffset(Info_in{
		TopicName:     req.Topic,
		PartitionName: req.Part,
		Index:         req.Offset,
	})
	if err != nil {
		return &api.UpdateOffsetResponse{
			Ret: false,
		}, err
	}
	return &api.UpdateOffsetResponse{
		Ret: true,
	}, nil
}

// broker---->zkserver，push中的addmessage的时候会用到
func (s *RPCServer) UpdateDup(ctx context.Context, req *api.UpdateDupRequest) (r *api.UpdateDupResponse, err error) {
	err = s.zkServer.UpdateDupHandle(Info_in{
		TopicName:     req.Topic,
		PartitionName: req.Part,
		CliName:       req.BrokerName,
		BlockName:     req.BlockName,
		Index:         req.EndIndex,
	})
	if err != nil {
		return &api.UpdateDupResponse{
			Ret: false,
		}, err
	}
	return &api.UpdateDupResponse{
		Ret: true,
	}, nil
}

// broker----->zkserver
// broker发送自己的地址和名字让zkserver进行连接
func (s *RPCServer) BroInfo(ctx context.Context, req *api.BroInfoRequest) (r *api.BroInfoResponse, err error) {
	err = s.zkServer.BroInfoHandle(req.BroName, req.BroHostPort)
	if err != nil {
		DEBUG(dError, err.Error())
		return &api.BroInfoResponse{
			Ret: false,
		}, err
	}
	return &api.BroInfoResponse{
		Ret: true,
	}, nil
}
func (s *RPCServer) BroGetAssign(ctx context.Context, req *api.BroGetAssignRequest) (r *api.BroGetAssignResponse, err error) {
	// 用于broker加载缓存
	return &api.BroGetAssignResponse{
		Ret: true,
	}, nil
}
func (s *RPCServer) CreateTopic(ctx context.Context, req *api.CreateTopicRequest) (r *api.CreateTopicResponse, err error) {
	info_out := s.zkServer.CreateTopicHandle(Info_in{
		TopicName: req.TopicName,
	})
	if info_out.Err != nil {
		return &api.CreateTopicResponse{
			Ret: false,
			Err: info_out.Err.Error(),
		}, info_out.Err
	}
	return &api.CreateTopicResponse{
		Ret: true,
		Err: "ok",
	}, nil
}
func (s *RPCServer) CreatePartition(ctx context.Context, req *api.CreatePartitionRequest) (r *api.CreatePartitionResponse, err error) {
	info_out := s.zkServer.CreatePartitionHandle(Info_in{
		TopicName:     req.TopicName,
		PartitionName: req.PartitionName,
	})
	if info_out.Err != nil {
		return &api.CreatePartitionResponse{
			Ret: false,
			Err: info_out.Err.Error(),
		}, info_out.Err
	}
	return &api.CreatePartitionResponse{
		Ret: true,
		Err: "ok",
	}, nil
}

// // 消费者	指示开始消费某个分区的消息，从某个 offset 开始
// func (s *RPCServer) StarttoGet(ctx context.Context, req *api.InfoGetRequest) (r *api.InfoGetResponse, err error) {

//		err = s.server.StartGet(Info{
//			topic:           req.Topic_Name,
//			partition:       req.Partition_Name,
//			consumer_ipname: req.Cli_Name,
//			offset:          req.Offset,
//		})
//		if err != nil {
//			return &api.InfoGetResponse{Ret: false}, err
//		}
//		return &api.InfoGetResponse{Ret: true}, nil
//	}
//
//	struct ConStartGetBroRequest{
//	    1:string topic
//	    2:string part
//	    3:i64 offset
//	    4:i8 option
//	    5:string cli_name
//	}
//
//	struct ConStartGetBroResponse{
//	    1:bool ret
//	    2:i64 size
//	    3:binary parts
//	}
//
// 感觉这个和上面func (s *RPCServer) StarttoGet(ctx context.Context, req *api.InfoGetRequest) (r *api.InfoGetResponse, err error) 这个很像
func (s *RPCServer) ConStartGetBro(ctx context.Context, req *api.ConStartGetBroRequest) (r *api.ConStartGetBroResponse, err error) {
	parts, size, err := s.zkServer.ConStartGetBroHandle(Info_in{
		TopicName:     req.Topic,
		PartitionName: req.Part,
		Index:         req.Offset,
		Option:        req.Option,
		CliName:       req.CliName,
	})
	if err != nil {
		return &api.ConStartGetBroResponse{
			Ret: false,
		}, err
	}
	return &api.ConStartGetBroResponse{
		Ret:   true,
		Size:  int64(size),
		Parts: parts,
	}, nil
}
func (s *RPCServer) BecomeLeader(ctx context.Context, req *api.BecomeLeaderRequest) (r *api.BecomeLeaderResponse, err error) {
	err = s.zkServer.BecomeLeaderHandle(Info_in{
		TopicName:     req.Topic,
		PartitionName: req.Part,
		CliName:       req.Broker,
	})
	if err != nil {
		return &api.BecomeLeaderResponse{
			Ret: false,
		}, err
	}
	return &api.BecomeLeaderResponse{
		Ret: true,
	}, nil
}
