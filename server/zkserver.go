package server

import (
	"Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/server_operations"
	"Wx_MQ/zookeeper"
	"context"
	"encoding/json"
	"hash/crc32"
	"sort"
	"strconv"
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

	consistent *ConsistentBro
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
	broker, block, _, err := zks.zk.GetNowPartBrokerNode(req.TopicName, req.PartitionName)
	PartitionNode, err := zks.zk.GetPartitionNode(req.TopicName, req.PartitionName)
	if err != nil {
		//
	}
	zks.rmu.RLock()
	Brokers := make(map[string]string)
	var ret string
	Dups := zks.zk.GetDuplicateNodes(block.Topic, block.Partition, block.Name)
	for _, dupNode := range Dups {
		BrokerNode, err := zks.zk.GetBrokerNode(dupNode.BrokerName)
		if err != nil {
			//
		}
		Brokers[dupNode.BrokerName] = BrokerNode.BroHostPort
	}
	data, err := json.Marshal(Brokers)
	for brokername, brohostport := range Brokers {
		zks.rmu.RLock()
		bro_cli, ok := zks.brokers[brokername]
		zks.rmu.RUnlock()
		if !ok {
			bro_cli, err := server_operations.NewClient(zks.Name, client.WithHostPorts(brohostport))
			if err != nil {
				//
			}
			zks.rmu.Lock()
			zks.brokers[brokername] = bro_cli
			zks.rmu.Unlock()
		}
		resp, err := bro_cli.PrepareAccept(context.Background(), &api.PrepareAcceptRequest{
			Topic_Name:     block.Topic,
			Partition_Name: block.Partition,
			File_Name:      block.FileName,
		})
		if err != nil || !resp.Ret {

		}
		if PartitionNode.Option == -2 {
			ret = "Partition state is -2"
		} else {
			resp, err := bro_cli.PrepareState(context.Background(), &api.PrepareStateRequest{
				TopicName:     block.Topic,
				PartitionName: block.Partition,
				State:         PartitionNode.Option,
				Brokers:       data,
			})
			if err != nil || !resp.Ret {
				//
			}
		}
	}
	return Info_out{
		Err:           err,
		broker_name:   broker.Name,
		bro_host_port: broker.BroHostPort,
		Ret:           ret,
	}

}
func (zks *ZKServer) ProSetPartStateHandle(info Info_in) Info_out {

	node, err := zks.zk.GetPartitionNode(info.TopicName, info.PartitionName)
	if err != nil {
		return Info_out{
			Err: err,
		}
	}
	if info.Option != node.Option {
		//其实我目前感觉这个函数完全没有必要
		index, err := zks.zk.GetPartBlockIndex(info.TopicName, info.PartitionName)
		if err != nil {
			return Info_out{
				Err: err,
			}
		}
		//进行更新
		zks.zk.UpdatePartitionNode(zookeeper.PartitionNode{
			Option: info.Option,
			//这个是为了找到路径
			Topic: info.TopicName,
			Name:  info.PartitionName,
			//我感觉下面这些都灭有变化呀，为什么要赋值？？？
			Index:    index,
			PTPIndex: node.PTPIndex,
			DupNum:   node.DupNum,
		})
	}
	var Dups []zookeeper.DuplicateNode
	var data_brokers []byte
	var ret string
	//说明还未创建任何状态
	if node.Option == -2 {
		switch info.Option {
		//说明他想变为raft
		case -1:
			//说明option没变

			//先创建上三个副本再说
			Dups, data_brokers = zks.GetDupsFromConsist(info)
			err = zks.BecomLeader(Info_in{
				TopicName:     info.TopicName,
				PartitionName: info.PartitionName,
				CliName:       Dups[0].BrokerName,
			})
			if err != nil {
				return Info_out{
					Err: err,
				}
			}
			//向这些broker发送信息，启动raft
			for _, dupnode := range Dups {
				bro_cli, ok := zks.brokers[dupnode.BrokerName]
				if !ok {
					//
				} else {
					//开启raft集群
					_, err := bro_cli.AddRaftPartition(context.Background(), &api.AddRaftPartitionRequest{
						TopicName: info.TopicName,
						PartName:  info.PartitionName,
						Brokers:   data_brokers,
					})
					if err != nil {
						return Info_out{
							Err: err,
						}
					}
				}
			}
			//那么就说明他想变为fetch
		default:
			Dups, data_brokers = zks.GetDupsFromConsist(info)
			LeaderBroker, err := zks.zk.GetBrokerNode(Dups[0].BrokerName)
			if err != nil {
				return Info_out{
					Err: err,
				}
			}
			for _, dupnode := range Dups {
				bro_cli, ok := zks.brokers[dupnode.BrokerName]
				if !ok {
					//
				} else {
					_, err := bro_cli.AddFetchPartition(context.Background(), &api.AddFetchPartitionRequest{
						TopicName:    info.TopicName,
						PartName:     info.PartitionName,
						Brokers:      data_brokers,
						LeaderBroker: LeaderBroker.Name,
						HostPort:     LeaderBroker.BroHostPort,
						FileName:     "NowBlock.txt",
					})
					if err != nil {
						return Info_out{
							Err: err,
						}
					}
				}
			}
			return Info_out{
				Ret: ret,
			}
		}
	}
	LeaderBroker, NowBlock, _, err := zks.zk.GetNowPartBrokerNode(info.TopicName, info.PartitionName)
	if err != nil {
		return Info_out{
			Err: err,
		}
	}
	Dups = zks.zk.GetDuplicateNodes(NowBlock.Topic, NowBlock.Partition, NowBlock.Name)
	var brokers BrokerS
	brokers.BroBrokers = make(map[string]string)
	brokers.RaftBrokers = make(map[string]string)
	for _, DupNode := range Dups {
		BrokerNode, err := zks.zk.GetBrokerNode(DupNode.BrokerName)
		if err != nil {
			//
		}
		brokers.BroBrokers[DupNode.BrokerName] = BrokerNode.BroHostPort
		brokers.RaftBrokers[DupNode.BrokerName] = BrokerNode.RaftHostPort
	}
	data_brokers, err = json.Marshal(brokers)
	if err != nil {
		return Info_out{
			Err: err,
		}
	}
	switch info.Option {
	case -1:
		if node.Option == -1 {
			ret = "HadRaft"
		}
		if node.Option == 1 || node.Option == 0 {
			for ice, dupnode := range Dups {
				lastfilename := zks.CloseAcceptPartition(info.TopicName, info.PartitionName, dupnode.BrokerName, ice)
				bro_cli, ok := zks.brokers[dupnode.BrokerName]
				if !ok {
					//
				} else {
					//关闭fetch机制
					_, err := bro_cli.CloseFetchPartition(context.Background(), &api.CloseFetchPartitionRequest{
						TopicName: info.TopicName,
						PartName:  info.PartitionName,
					})
					if err != nil {
						return Info_out{
							Err: err,
						}
					}
					//重新准备接收文件
					_, err = bro_cli.PrepareAccept(context.Background(), &api.PrepareAcceptRequest{
						Topic_Name:     info.TopicName,
						Partition_Name: info.PartitionName,
						File_Name:      "NowBlock.txt",
					})
					if err != nil {
						return Info_out{
							Err: err,
						}
					}
					//开启raft集群
					_, err = bro_cli.AddRaftPartition(context.Background(), &api.AddRaftPartitionRequest{
						TopicName: info.TopicName,
						PartName:  info.PartitionName,
						Brokers:   data_brokers,
					})
					if err != nil {
						return Info_out{
							Err: err,
						}
					}
					//开启fetch机制
					_, err = bro_cli.AddFetchPartition(context.Background(), &api.AddFetchPartitionRequest{
						TopicName:    info.TopicName,
						PartName:     info.PartitionName,
						Brokers:      data_brokers,
						LeaderBroker: LeaderBroker.Name,
						HostPort:     LeaderBroker.BroHostPort,
						FileName:     lastfilename,
					})
					if err != nil {
						return Info_out{
							Err: err,
						}
					}
				}
			}
		}
	default:
		if node.Option != -1 {
			ret = "HadFetch"
		} else {
			for _, dupnode := range Dups {
				bro_cli, ok := zks.brokers[dupnode.BrokerName]
				if !ok {
					//
				} else {
					_, err := bro_cli.CloseRaftPartition(context.Background(), &api.CloseRaftPartitionRequest{
						TopicName: info.TopicName,
						PartName:  info.PartitionName,
					})
					if err != nil {
						return Info_out{
							Err: err,
						}
					}
					_, err = bro_cli.AddFetchPartition(context.Background(), &api.AddFetchPartitionRequest{
						TopicName:    info.TopicName,
						PartName:     info.PartitionName,
						Brokers:      data_brokers,
						LeaderBroker: LeaderBroker.Name,
						HostPort:     LeaderBroker.BroHostPort,
						FileName:     "NowBlock.txt",
					})
					if err != nil {
						return Info_out{
							Err: err,
						}
					}
				}
			}
		}
	}
	return Info_out{
		Ret: ret,
	}
}
func (zks *ZKServer) GetDupsFromConsist(info Info_in) (Dups []zookeeper.DuplicateNode, data_brokers []byte) {
	str := info.TopicName + "/" + info.PartitionName
	Bro_dups := zks.consistent.GetNode(str+"dup", 3)
	Dups = append(Dups, zookeeper.DuplicateNode{
		Name:        "dup_0",
		Topic:       info.TopicName,
		Partition:   info.PartitionName,
		BrokerName:  Bro_dups[0],
		StartOffset: int64(0),
		BlockName:   "NowBlock",
	})
	Dups = append(Dups, zookeeper.DuplicateNode{
		Name:        "dup_1",
		Topic:       info.TopicName,
		Partition:   info.PartitionName,
		BrokerName:  Bro_dups[1],
		StartOffset: int64(0),
		BlockName:   "NowBlock",
	})
	Dups = append(Dups, zookeeper.DuplicateNode{
		Name:        "dup_2",
		Topic:       info.TopicName,
		Partition:   info.PartitionName,
		BrokerName:  Bro_dups[2],
		StartOffset: int64(0),
		BlockName:   "NowBlock",
	})
	for _, dup := range Dups {
		err := zks.zk.RegisterNode(dup)
		if err != nil {
			//
		}
	}
	var brokers BrokerS
	brokers.BroBrokers = make(map[string]string)
	brokers.RaftBrokers = make(map[string]string)
	brokers.Me_Brokers = make(map[string]int)
	for _, DupNode := range Dups {
		BrokerNode, err := zks.zk.GetBrokerNode(DupNode.BrokerName)
		if err != nil {
			//
		}
		brokers.BroBrokers[BrokerNode.Name] = BrokerNode.BroHostPort
		brokers.RaftBrokers[BrokerNode.Name] = BrokerNode.BroHostPort
		brokers.Me_Brokers[BrokerNode.Name] = BrokerNode.Me
	}
	data_brokers, err := json.Marshal(brokers)
	if err != nil {
		//
	}
	return Dups, data_brokers
}
func (zks *ZKServer) BecomLeader(info Info_in) error {
	now_block_path := zks.zk.TopicRoot + "/" + info.TopicName + "/" + "partitions" + "/" + info.PartitionName + "/" + "NowBlock"
	NowBlock, err := zks.zk.GetBlockNode(now_block_path)
	if err != nil {
		//
	}
	NowBlock.LeaderBroker = info.CliName
	return zks.zk.UpdateBlockNode(NowBlock)
}
func (zks *ZKServer) CloseAcceptPartition(topicname, partname, brokername string, ice int) string {
	index, err := zks.zk.GetPartBlockIndex(topicname, partname)
	if err != nil {
		return err.Error()
	}
	NewBlockName := "Block_" + strconv.Itoa(int(index))
	NewFileName := NewBlockName + "txt"
	zks.rmu.Lock()
	bro_cli, ok := zks.brokers[brokername]
	if !ok {
		//
	} else {
		resp, err := bro_cli.CloseAccept(context.Background(), &api.CloseAcceptRequest{
			Topic_Name:     topicname,
			Partition_Name: partname,
			OldFile_Name:   "NoeBlock.txt",
			NewFile_Name_:  NewFileName,
		})
		if err != nil && !resp.Ret {
			//
		} else {
			str := zks.zk.TopicRoot + "/" + topicname + "/" + "Partitions" + "/" + partname + "/" + "NowBlock"
			bnode, err := zks.zk.GetBlockNode(str)
			if err != nil {
				//
			}
			if ice == 0 {
				//创造新节点
				zks.zk.RegisterNode(zookeeper.BlockNode{
					Name:         NewBlockName,
					Topic:        topicname,
					Partition:    partname,
					FileName:     NewFileName,
					StartOffset:  resp.Startindex,
					EndOffset:    resp.Endindex,
					LeaderBroker: bnode.LeaderBroker,
				})
				//更新原NowBlock节点信息
				zks.zk.UpdateBlockNode(zookeeper.BlockNode{
					Name:         NewBlockName,
					Topic:        topicname,
					Partition:    partname,
					FileName:     NewFileName,
					StartOffset:  resp.Startindex,
					EndOffset:    resp.Endindex,
					LeaderBroker: bnode.LeaderBroker,
				})
			}
			//创建该节点下的各个Dup节点
			DupPath := zks.zk.TopicRoot + "/" + topicname + "/" + "Partitions" + "/" + partname + "/" + "NowBlock" + "/" + brokername
			DupNode, err := zks.zk.GetDuplicateNode(DupPath)
			if err != nil {
				//
			}
			DupNode.BlockName = NewBlockName
			zks.zk.RegisterNode(DupNode)
		}
	}
	zks.rmu.RUnlock()
	return NewFileName
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
	if info.Option == TOPIC_NIL_PTP_PULL || info.Option == TOPIC_NIL_PTP_PUSH {
		Parts, err = zks.zk.GetBrokers(info.TopicName)
	} else if info.Option == TOPIC_KEY_PSB_PULL || info.Option == TOPIC_KEY_PSB_PUSH {
		Parts, err = zks.zk.GetBroker(info.TopicName, info.PartitionName, info.Index)
	}
	if err != nil {
		return nil, 0, err
	}
	var partkeys []clients.PartKey
	partkeys = zks.SendPrepare(Parts, info)
	//没太看懂这里
	parts := clients.Parts{
		Parts: partkeys,
	}
	data, err := json.Marshal(parts)
	var nodes clients.Parts

	json.Unmarshal(data, &nodes)

	if err != nil {
		//
	}

	//--------------------------------我想的是下面这个把上面哪个替换
	//data, err := json.Marshal(partkeys)
	return data, len(data), nil

}
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
				//
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
			Consumer:       info.CliName,
		}
		if info.Option == TOPIC_NIL_PTP_PULL || info.Option == TOPIC_NIL_PTP_PUSH {
			req.Offset = part.PTPIndex
		} else if info.Option == TOPIC_KEY_PSB_PULL || info.Option == TOPIC_KEY_PSB_PUSH {
			req.Offset = info.Index
		}
		resp, err := bro_cli.PrepareSend(context.Background(), req)
		if err != nil || !resp.Ret {
			//
		}
		part := clients.PartKey{
			Name:       part.PartitionName,
			BrokerName: part.BrokerName,
			BrokerHP:   part.BroHostPort,
			Err:        OK,
		}
		partkeys = append(partkeys, part)
	}
	return partkeys
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

type ConsistentBro struct {
	rmu              sync.RWMutex
	hashSortNodes    []uint32          //排序的虚拟节点
	circleNodes      map[uint32]string //虚拟节点对应的世纪节点
	virtualNodeCount int               //虚拟节点数量
	nodes            map[string]bool   //已绑定的世纪节点为true，这个还不太了解
	//consumer以负责一个partition则为true
	BroH map[string]bool
}

func (c *ConsistentBro) GetNode(key string, num int) (dups []string) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	c.SetBroHFalse()
	hashKey := c.hashKey(key)
	for index := 0; index < num; index++ {
		i := c.getposition(hashKey)
		bro_name := c.circleNodes[c.hashSortNodes[i]]
		c.BroH[bro_name] = true
		dups = append(dups, bro_name)
	}
	return
}
func (c *ConsistentBro) SetBroHFalse() {
	for k, _ := range c.BroH {
		c.BroH[k] = false
	}
}
func (c *ConsistentBro) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
func (c *ConsistentBro) getposition(hashKey uint32) (ret int) {
	i := sort.Search(len(c.hashSortNodes), func(i int) bool {
		return c.hashSortNodes[i] >= hashKey
	})
	if i < len(c.hashSortNodes) {
		if i == len(c.hashSortNodes)-1 {
			ret = 0
		} else {
			ret = i
		}
	} else {
		ret = len(c.hashSortNodes) - 1
	}
	for c.BroH[c.circleNodes[c.hashSortNodes[ret]]] {
		ret++
	}
	return
}
