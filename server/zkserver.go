package server

import (
	"Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/server_operations"
	"Wx_MQ/zookeeper"
	"context"
	"encoding/json"
	"sync"

	"Wx_MQ/client/clients"

	"github.com/cloudwego/kitex/client"
	"github.com/docker/docker/client"
)

type ZKServer struct {
	rmu sync.RWMutex
	zk  zookeeper.ZK

	info_brokers   map[string]zookeeper.BrokerNode
	info_topics    map[string]zookeeper.TopicNode
	info_partition map[string]zookeeper.PartitionNode

	brokers map[string]server_operations.Client
	Name    string
}

func NewZKServer(zkInfo zookeeper.ZKInfo) *ZKServer {
	return &ZKServer{
		rmu:            sync.RWMutex{},
		zk:             *zookeeper.NewZK(zkInfo),
		info_brokers:   make(map[string]zookeeper.BrokerNode),
		info_topics:    make(map[string]zookeeper.TopicNode),
		info_partition: make(map[string]zookeeper.PartitionNode),
		brokers:        make(map[string]server_operations.Client),
	}
}
func (zks *ZKServer) make(opt Options) {
	zks.Name = opt.Name
}

type Info_in struct {
	TopicName     string
	PartitionName string
	Option        int8
	CliName       string
	Index         int64
	Dupnum        int8
}
type Info_out struct {
	Err           error
	broker_name   string
	bro_host_port string
	Ret           string
}

// type PrepareAcceptRequest struct {
// 	Topic_Name     string `thrift:"topic_Name,1" frugal:"1,default,string" json:"topic_Name"`
// 	Partition_Name string `thrift:"partition_Name,2" frugal:"2,default,string" json:"partition_Name"`
// 	File_Name      string `thrift:"file_Name,3" frugal:"3,default,string" json:"file_Name"`
// }

func (zks *ZKServer) ProGetBroHandle(req Info_in) Info_out {
	broker, block := zks.zk.GetNowPartBrokerNode(req.TopicName, req.PartitionName)
	zks.rmu.RLock()
	bro_cli, ok := zks.brokers[broker.Name]
	zks.rmu.RUnlock()
	if !ok {
		bro_cli, err := server_operations.NewClient(zks.Name, client.WithHostPorts(broker.Host+broker.Port))
		if err != nil {

		}
		zks.rmu.Lock()
		zks.brokers[broker.Name] = bro_cli
		zks.rmu.Unlock()
	}
	//给broker说明准备好接收消息
	resp, err := bro_cli.PrepareAccept(context.Background(), &api.PrepareAcceptRequest{
		Topic_Name:     req.TopicName,
		Partition_Name: req.PartitionName,
		File_Name:      block.FileName,
	})
	if err != nil || !resp.Ret {

	}
	return Info_out{
		broker_name:   broker.Name,
		bro_host_port: broker.Host + broker.Port,
	}

}
func (zks *ZKServer) ProSetPartStateHandle(info Info_in) Info_out {
	node, _ := zks.zk.GetPartitionNode(info.TopicName, info.PartitionName)
	if info.Option != node.Option {
		//进行更新
		zks.zk.UpdatePartitionNode(zookeeper.PartitionNode{
			Topic:    info.TopicName,
			Name:     info.PartitionName,
			Option:   info.Option,
			PTPIndex: info.Index,
		})
	}
	var ret string
	switch info.Option {
	//说明他想变为raft
	case -1:
		//说明option没变
		if node.Option == -1 {
			ret = "HadRaft"
		}
		if node.Option == 0 || node.Option == 1 {
			//说明状态由之前的fetch变为reft
		}
		//还有这个状态？？
		if node.Option == -2 {

		}
		//那么就说明他想变为fetch
	default:
		if node.Option == 0 || node.Option == 1 {
			ret = "HadFetch"
		}
		if node.Option == -1 {
			//说明状态由之前的raft变为fetch
		}
		if node.Option == -2 {

		}
	}
	return Info_out{
		Ret: ret,
	}
}
func (zks *ZKServer) ConGetBroHandle(info Info_in) (rets []byte, size int, err error) {
	//检查该用户是否订阅了topic/part
	zks.zk.CheckSub(zookeeper.StartGetInfo{
		CliName:       info.CliName,
		TopicName:     info.TopicName,
		PartitionName: info.PartitionName,
		Option:        info.Option,
	})
	var Parts []zookeeper.Part
	//这两个的区别：
	//ptp:他在指定topic里面的所有partition里面找这个partition对应PTP_INDEX-系统找到的正消费到的-的block
	//psb:特定topic特定partition的特定index-consumer指定
	if info.Option == 1 {
		Parts, err = zks.zk.GetBrokers(info.TopicName)
	} else if info.Option == 3 {
		Parts, err = zks.zk.GetBroker(info.TopicName, info.PartitionName, info.Index)
	}
	if err != nil {
		return nil, 0, err
	}
	var partkeys []clients.PartKey
	for _, part := range Parts {
		zks.rmu.RLock()
		bro_cli, ok := zks.brokers[part.BrokerName]
		zks.rmu.RUnlock()
		if !ok {
			bro_cli, err = server_operations.NewClient(zks.Name, client.WithHostPorts(part.BroHostPort))
			if err != nil {
				return nil, 0, err
			}
			zks.rmu.Lock()
			zks.brokers[part.BrokerName] = bro_cli
			zks.rmu.Unlock()
		}
		req := &api.PrepareSendRequest{
			Topic_Name:     info.TopicName,
			Partition_Name: info.PartitionName,
			File_Name:      part.FileName,
			Option:         info.Option,
		}
		if info.Option == 1 {
			req.Offset = part.PTPIndex
		} else if info.Option == 3 {
			req.Offset = info.Index
		}
		resp, err := bro_cli.PrepareSend(context.Background(), req)
		if err != nil || !resp.Ret {
			return nil, 0, err
		}
		part := clients.PartKey{
			Name:       part.PartitionName,
			BrokerName: part.BrokerName,
			BrokerHP:   part.BroHostPort,
		}
		partkeys = append(partkeys, part)
	}

	data, err := json.Marshal(partkeys)
	return data, len(data), nil

}
func (zks *ZKServer) SubHandle(sub Info_in) error {

}

// 创建一个新的broker
func (zks *ZKServer) BroInfoHandle(broname, brohostport string) error {
	brocli, err := server_operations.NewClient(zks.Name, client.WithHost(brohostport))
	zks.rmu.Lock()
	zks.brokers[broname] = brocli
	zks.rmu.Unlock()
	return err
}
func (zks *ZKServer) CreateTopicHandle(topic Info_in) Info_out {
	tNode := zookeeper.TopicNode{
		Name: topic.TopicName,
	}
	err := zks.zk.RegisterNode(tNode)
	return Info_out{
		Err: err,
	}
}
func (zks *ZKServer) CreatePartitionHandle(part Info_in) Info_out {
	pNode := zookeeper.PartitionNode{
		Name:     part.PartitionName,
		Topic:    part.TopicName,
		PTPIndex: int64(0),
	}
	err := zks.zk.RegisterNode(pNode)
	err = zks.CreateNowBlock(part)
	return Info_out{
		Err: err,
	}
}

// ------------------------------------------------------------------
func (zks *ZKServer) CreateNowBlock(block Info_in) error {
	bNode := zookeeper.BlockNode{
		Name:        "nowBlock",
		Topic:       block.TopicName,
		Partition:   block.PartitionName,
		StartOffset: int64(0),
		FileName:    block.TopicName + "/" + block.PartitionName + "now.txt",
	}
	err := zks.zk.RegisterNode(bNode)
	return err
}

// 自己感觉这里是可以简化的
func (zks *ZKServer) UpdatePTPOffset(info Info_in) error {
	err := zks.zk.UpdatePartitionNode(zookeeper.PartitionNode{
		Name:     info.PartitionName,
		Topic:    info.TopicName,
		PTPIndex: info.Index,
	})
	return err
}
