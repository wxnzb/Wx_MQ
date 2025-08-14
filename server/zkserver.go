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
	rmu  sync.RWMutex
	zk   zookeeper.ZK
	Name string

	info_brokers   map[string]zookeeper.BrokerNode
	info_topics    map[string]zookeeper.TopicNode
	info_partition map[string]zookeeper.PartitionNode
	//连接各个brokers
	brokers map[string]server_operations.Client
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
	BlockName     string
	Index         int64
	Dupnum        int8
}
type Info_out struct {
	Err           error
	broker_name   string
	bro_host_port string
	Ret           string
}

// 更新 Zookeeper 中某个 partition 的“数据块”与“副本”的处理进度（即 EndOffset）
func (zks *ZKServer) UpdateDupHandle(in Info_in) error {
	str := zks.zk.TopicRoot + "/" + in.TopicName + "/" + in.PartitionName + "/" + in.BlockName
	BlockNode, err := zks.zk.GetBlockNode(str)
	if err != nil {
		return err
	}
	// 如果当前 Broker 上报的 index 比记录的 EndOffset 大：
	// 更新 BlockNode 的 EndOffset，表示这个数据块写入更多了
	// 将更新后的 BlockNode 注册（写回）zk
	if in.Index > BlockNode.EndOffset {
		BlockNode.EndOffset = in.Index
		err = zks.zk.RegisterNode(BlockNode)
		if err != nil {
			return err
		}
	}
	//获取这个 Broker 对应的 副本节点
	DupNode, err := zks.zk.GetDuplicateNode(str + "/" + in.CliName)
	if err != nil {
		return err
	}
	DupNode.EndOffset = in.Index
	err = zks.zk.RegisterNode(DupNode)
	if err != nil {
		return err
	}
	return nil
}

// type PrepareAcceptRequest struct {
// 	Topic_Name     string `thrift:"topic_Name,1" frugal:"1,default,string" json:"topic_Name"`
// 	Partition_Name string `thrift:"partition_Name,2" frugal:"2,default,string" json:"partition_Name"`
// 	File_Name      string `thrift:"file_Name,3" frugal:"3,default,string" json:"file_Name"`
// }

func (zks *ZKServer) ProGetBroHandle(req Info_in) Info_out {
	broker, block := zks.zk.GetNowPartBrokerNode(req.TopicName, req.PartitionName)
	zks.rmu.RLock()
	bro_cli, ok := zks.brokers[block.LeaderBroker]
	zks.rmu.RUnlock()
	if !ok {
		bro_cli, err := server_operations.NewClient(zks.Name, client.WithHostPorts(broker.HostPort))
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
		bro_host_port: broker.HostPort,
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
	// 在zookeeper上创建sub节点，要是节点已经存在，就加入group
	return nil
}

// 将broker发送过来想要连接的名字，通过地址和名字联系起来，也就是两者有了联系
func (zks *ZKServer) BroInfoHandle(broname, brohostport string) error {
	brocli, err := server_operations.NewClient(zks.Name, client.WithHostPorts(brohostport))
	if err != nil {
		DEBUG(dError, err.Error())
	}
	zks.rmu.Lock()
	zks.brokers[broname] = brocli
	zks.rmu.Unlock()
	return nil
}

// func (s *Server) StartGet(req Info) (err error) {
// 	//先看
// 	switch req.option {
// 	//负载均衡
// 	case TOPIC_NIL_PTP:
// 		{
// 			s.rmu.Lock()
// 			defer s.rmu.Unlock()
// 			//先看这个是否订阅
// 			sub_name := GetStringFromSub(req.topic, req.partition, req.option)
// 			ret := s.consumers[req.consumer_ipname].CheckSubscription(sub_name)
// 			sub := s.consumers[req.consumer_ipname].GetSub(sub_name)
// 			if ret == true {
// 				sub.AddConsumerInConfig(req, s.consumers[req.consumer_ipname].GetToConsumer())
// 			} else {
// 				err = errors.New("not subscribe")
// 			}
// 		}
// 		//广播
// 	case TOPIC_KEY_PSB:
// 		{
// 			s.rmu.Lock()
// 			defer s.rmu.Unlock()
// 			//先看这个是否订阅
// 			sub_name := GetStringFromSub(req.topic, req.partition, req.option)
// 			ret := s.consumers[req.consumer_ipname].CheckSubscription(sub_name)
// 			if ret == true {
// 				//下面这三行是为了没有这个分区的时候新建一个
// 				// toConsumers := make(map[string]*client_operations.Client)
// 				// toConsumers[req.consumer_ipname] = s.consumers[req.consumer_ipname].GetToConsumer()
// 				// file := s.topics[req.topic].GetFile(req.partition)
// 				// go s.consumers[req.consumer_ipname].StartPart(req, toConsumers, file)
// 			} else {
// 				err = errors.New("not subscribe")
// 			}
// 		}
// 	default:
// 		err = errors.New("option error")

//		}
//		return err
//	}
//
// 感觉和server的这个差不多，先看看
func (zks *ZKServer) ConStartGetBroHandle(in Info_in) (rets []byte, size int, err error) {

	//先检查这个consumer收否订阅了这个topic/partition
	zks.zk.CheckSub(zookeeper.StartGetInfo{
		CliName:       in.CliName,
		TopicName:     in.TopicName,
		PartitionName: in.PartitionName,
		Option:        in.Option,
	})
	//获取topic或者part的broker,需要保证在先，要是全部离线则ERR
	var Parts []zookeeper.Part
	if in.Option == 1 {
		Parts, err = zks.zk.GetBrokers(in.TopicName)
	} else if in.Option == 3 {
		Parts, err = zks.zk.GetBroker(in.TopicName, in.PartitionName, in.Index)
	}
	if err != nil {
		return nil, 0, err
	}
	//-----------------
	partkeys := zks.SendPrepare(Parts, in)
	data, err := json.Marshal(partkeys)
	if err != nil {
		DEBUG(dError, "turn partkeys to json fail %v", err.Error())
	}
	return data, len(partkeys), nil

}

// 对一组分区 (Parts) 所属的 broker 发起 “PrepareSend” 请求，准备将某段数据推送过去，并记录结果（partkeys）返回
func (zks *ZKServer) SendPrepare(Parts []zookeeper.Part, info Info_in) (partkeys []clients.PartKey) {
	for _, part := range Parts {
		if part.Err != OK {
			partkeys = append(partkeys, clients.PartKey{
				Err: part.Err,
			})
			continue
		}
		zks.rmu.RLock()
		bro_cli, ok := zks.brokers[part.BrokerName]
		zks.rmu.RUnlock()
		if !ok {
			bro_cli, err := server_operations.NewClient(zks.Name, client.WithHostPorts(part.BroHostPort))
			if err != nil {
				return nil
			}
			zks.rmu.Lock()
			zks.brokers[part.BrokerName] = bro_cli
			zks.rmu.Unlock()
		}
		rep := &api.PrepareSendRequest{
			Topic_Name:     info.TopicName,
			Partition_Name: info.PartitionName,
			File_Name:      part.FileName,
			Option:         info.Option,
		}
		if info.Option == 1 {
			rep.Offset = part.PTPIndex
		} else if info.Option == 3 {
			rep.Offset = info.Index
		}
		resp, err := bro_cli.PrepareSend(context.Background(), rep)
		if err != nil || !resp.Ret {
			return nil
		}
		partkeys = append(partkeys, clients.PartKey{
			Name:       part.PartitionName,
			BrokerName: part.BrokerName,
			BrokerHP:   part.BroHostPort,
			Err:        OK,
		})
	}
	return partkeys
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
		Option:   -2,
		Index:    int64(1),
	}
	err := zks.zk.RegisterNode(pNode)
	if err != nil {
		return Info_out{
			Err: err,
		}
	}
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
func (zks *ZKServer) UpdateOffset(info Info_in) error {
	err := zks.zk.UpdatePartitionNode(zookeeper.PartitionNode{
		Name:     info.PartitionName,
		Topic:    info.TopicName,
		PTPIndex: info.Index,
	})
	return err
}
