package server

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/raft_operations"
	"Wx_MQ/kitex_gen/api/server_operations"
	"Wx_MQ/kitex_gen/api/zkserver_operations"
	"Wx_MQ/logger"
	"Wx_MQ/zookeeper"
	"errors"

	"github.com/cloudwego/kitex/client"
	cl "github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/server"

	//"github.com/docker/docker/client"
	"context"
)

type NodeData struct {
	Start_index int64 `json:"start_index"` //这几批消息的第一批消息的index
	End_index   int64 `json:"end_index"`   //这几批消息的最后一批消息的index
	Size        int   `json:"size"`        //这几批消息的总大小
}

const (
	NODE_SIZE = 42
)

type Message struct {
	Topic_name     string `json:topic_name`
	Partition_name string `json:partition_name`
	Index          int64  `json:"index"`
	Msg            []byte `json:msg`
	Size           int8   `json:size`
}
type Msgs struct {
	producer string
	topic    string
	key      string
	msgs     []byte
}

// -------------------------------
type Server struct {
	topics      map[string]*Topic
	consumers   map[string]*ToConsumer //这里的string是消费者的ip_port
	rmu         sync.RWMutex
	zkclient    zkserver_operations.Client
	zk          zookeeper.ZK
	name        string
	parts_rafts *parts_raft
	//这里的brokers为了达到raft共识,k:其他
	brokers map[string]*raft_operations.Client
	aplych  chan Info
	me      int
	//fetch
	parts_fetch   map[string]string
	brokers_fetch map[string]*server_operations.Client
}

var ip_name string //加了这个,这个应该broker的ipport
// 其实感觉挺奇怪的，这里为什么不直接把这个放到make里面呢？？？？
func NewServer(zkInfo zookeeper.ZKInfo) *Server {
	return &Server{
		rmu: sync.RWMutex{},
		zk:  *zookeeper.NewZK(zkInfo),
	}
}

// 这个broker先和zk建立联系，将这个broker向zk进行注册
func (s *Server) make(opt Options, opt_cli []server.Option) {
	s.topics = make(map[string]*Topic)
	s.consumers = make(map[string]*ToConsumer)
	s.brokers = make(map[string]*raft_operations.Client)
	s.parts_fetch = make(map[string]string)
	s.brokers_fetch = make(map[string]*server_operations.Client)
	s.aplych = make(chan Info)

	//ip_name = GetIpPort() //本地ipport
	//为 当前 Broker 节点 创建一个 本地存储目录。目录名是 当前工作目录/ip_name,这是为了把broker的日志什么的存进去
	s.CheckList()
	s.name = opt.Name
	s.me = opt.Me

	//在这个节点的后台启动一个raft
	s.parts_rafts = NewPartRaft()
	go s.parts_rafts.make(opt.Name, opt_cli, s.aplych, s.me)
	s.parts_rafts.StartServer()
	//向zk注册自己，也就是让
	s.zkclient, _ = zkserver_operations.NewClient(opt.Name, cl.WithHostPorts(opt.ZKServerHostPort))
	resp, err := s.zkclient.BroInfo(context.Background(), &api.BroInfoRequest{
		BroName:     opt.Name,
		BroHostPort: opt.BrokerHostPort,
	})
	if resp.Ret == false || err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	//s.InitBroker()
	//创建一个永久节点在zk中，/brokerroot/brokername里面存放了这个broker的json的name和ipport，作用：要是broker挂了，历史数据在者可以找到；用于其他节点的发现
	err = s.zk.RegisterNode(zookeeper.BrokerNode{
		Name:         s.name,
		Me:           s.me,
		BroHostPort:  opt.BrokerHostPort,
		RaftHostPort: opt.RaftHostPort,
	})
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	//创建临时节点，表示当前Broker 是否在线，为什么要用，broker挂了，但是永久节点还在，其他节点无法判断这个broker的状态，所以可以通过这个函数检查这个broker的状态
	err = s.zk.CreateState(s.name)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	//开启获取管道中的内容，写入文件或更新leader
	go s.GetApplych(s.aplych)
}

// 接收applych管道的内容
// 写入partition文件中
func (s *Server) GetApplych(applych chan Info) {

	for msg := range applych {

		if msg.producer == "Leader" {
			s.BecomeLeader(msg) //成为leader
		} else {
			s.rmu.RLock()
			topic, ok := s.topics[msg.topic]
			s.rmu.RUnlock()

			logger.DEBUG(logger.DLog, "S%d the message from applych is %v\n", s.me, msg)
			if !ok {
				logger.DEBUG(logger.DError, "topic(%v) is not in this broker\n", msg.topic)
			} else {
				msg.me = s.me
				msg.BrokerName = s.name
				msg.zkclient = &s.zkclient
				msg.file_name = "NowBlock.txt"
				topic.AddMessage(msg) //信息同步
			}
		}
	}
}
func (s *Server) BecomeLeader(in Info) {
	resp, err := s.zkclient.BecomeLeader(context.Background(), &api.BecomeLeaderRequest{
		Broker: s.name,
		Topic:  in.topic,
		Part:   in.partition,
	})
	if err != nil || !resp.Ret {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
}
func (s *Server) InitBroker() {
	s.rmu.Lock()
	broker_power := Broker_Power{
		Name:  s.name,
		Power: 1,
	}
	data, err := json.Marshal(broker_power)
	if err != nil {

	}
	resp, err := s.zkclient.BroGetAssign(context.Background(), &api.BroGetAssignRequest{
		Brokerpower: data,
	})
	broker_assign := Broker_Assign{
		Topics: make(map[string]Top_Info),
	}
	err = json.Unmarshal(resp.Assignment, &broker_assign)
	s.HandleTopics(broker_assign.Topics)
	s.rmu.Unlock()
}

//下面这些是根据broker的信息重新分配t-p

func (s *Server) HandleTopics(topics map[string]Top_Info) {
	for topic_name, topic := range topics {
		_, ok := s.topics[topic_name]
		if !ok {
			top := NewTopic(s.name, topic_name)
			top.HandlePartitions(topic.Partitions)
			s.topics[topic_name] = top
		} else {
			logger.DEBUG(logger.DWarn, "This topic(%v) had in s.topics\n", topic_name)
		}
	}
}

// 这个还没有实现
func (s *Server) HandleBlocks(topic_name, partition_name string, blocks_info map[string]Block_Info) {

}

// 检查一个名为 ip_name 的目录是否存在，如果不存在就创建它
func (s *Server) CheckList() {
	str, _ := os.Getwd()
	str += "/" + ip_name
	ok := FileOrDirExist(str)
	//要是不存在，就创建一个
	if !ok {
		CreateDir(str)
	}
}

// 循环遍历这个消费者是否还在先，要是没在先，就要将他所有的订阅都删除调并重新进行平衡
func (s *Server) CheckConsumer(toconsumer *ToConsumer) {
	shutDown := toconsumer.CheckConsumer()
	if shutDown {
		toconsumer.rmu.Lock()
		for _, subscription := range toconsumer.subList {
			subscription.shutDownConsumer(toconsumer.name)
			//我现在感觉s.topics[subscription.topic_name].Rebalance()这样写不是更简单吗,还不用给shutDownConsumer这个函数加返回值，恩呢，感觉确实可以
			//s.topics[topic_name].Rebalance()
			//这里还要将consumer中的part关掉
		}
		toconsumer.rmu.Unlock()
	}
}

// 上面的要是失败了死了就取消，现在又要将他变成活得
func (s *Server) RecoverConsumer(consumer *ToConsumer) {
	s.rmu.Lock()
	consumer.rmu.Lock()
	consumer.state = ALIVE
	for sub_name, sub := range consumer.subList {
		//这里topic的RecoverConsumer还没有实现
		go s.topics[sub.topic_name].RecoverConsumer(sub_name, consumer)
	}
	consumer.rmu.Unlock()
	s.rmu.Unlock()
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
// ------------------------------------------------
// 1

// 服务器将消息存在topic里面
func (s *Server) PushHandle(push Info) (ret string, err error) {
	logger.DEBUG(logger.DLog, "get Message from producer\n")
	s.rmu.RLock()
	topic, ok := s.topics[push.topic]
	broker_part_raft := s.parts_rafts
	s.rmu.RUnlock()

	if !ok {
		ret = "this topic is not in this broker"
		logger.DEBUG(logger.DError, "Topic %v,is not in this broker", push.topic)
		return ret, errors.New(ret)
	}

	switch push.ack {
	//生产者要求 消息必须经过 Raft 共识（同步复制到多数节点）后再返回成功
	case -1:
		ret, err = broker_part_raft.Append(push)
		//直接写入本地（Leader）Topic 的消息队列。不等待 Raft 复制。
	case 1:
		err = topic.AddMessage(push)
	case 0: //直接返回
		go topic.AddMessage(push)
	}
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err.Error(), err
	}
	return ret, nil
}

// 2
// 先将他方在这里
// 一批消息
type MSGS struct {
	start_index int64
	end_index   int64
	size        int8
	array       []byte
}

func (s *Server) PullHandle(pullRequest Info) (MSGS, error) {
	logger.DEBUG(logger.DLog, "%v get pull request the in.op(%v) TOP_PTP_PULL(%v)\n", s.name, pullRequest.option, TOPIC_NIL_PTP_PULL)
	if pullRequest.option == TOPIC_NIL_PTP_PULL {
		//更新消费者偏移量并写入zookeeper记录消费者这次读取的位置,也就是要从哪里开始拉取
		s.zkclient.UpdateOffset(context.Background(), &api.UpdateOffsetRequest{
			Topic:  pullRequest.topic,
			Part:   pullRequest.partition,
			Offset: pullRequest.offset,
		})
	}
	s.rmu.RLock()
	topic, ok := s.topics[pullRequest.topic]
	s.rmu.RUnlock()
	if !ok {
		logger.DEBUG(logger.DError, "this topic is not in this broker")
		return MSGS{}, errors.New("topic not exist")
	}
	return topic.PullMessage(pullRequest)
}

// 处理消费者的连接请求,将消费者添加到这个broker里面
func (s *Server) InfoHandle(ip_port string) (err error) {
	logger.DEBUG(logger.DLog, "get consumer's ip_port %v\n", ip_port)
	s.rmu.Lock()
	consumer, ok := s.consumers[ip_port]
	if !ok {
		consumer, err = NewToConsumer(ip_port)
		if err != nil {
			return err
		}
		s.consumers[ip_port] = consumer
	}
	go s.CheckConsumer(consumer)
	s.rmu.Unlock()
	logger.DEBUG(logger.DLog, "return resp to consumer\n")
	return nil
}

// 4
type Info struct {
	name            string
	topic           string
	partition       string
	consumer_ipname string
	option          int8
	offset          int64
	size            int8
	file_name       string
	newfile_name    string
	producer        string
	consumer        string
	message         []byte
	ack             int8
	//cmdindex：幂等编号，防止重复提交
	cmdindex int64
	//raft:这里的k-broker名字，v-raft地址
	brokers map[string]string
	brok_me map[string]int
	//这个是干什么的？？
	me int
	//fetch
	LeaderBroker string
	HostPort     string
	//update dup
	zkclient   *zkserver_operations.Client
	BrokerName string
}

func (s *Server) StartGet(req Info) (err error) {
	/*
		新开启一个consumer关于一个topic和partition的协程来消费该partition的信息；

		查询是否有该订阅的信息；

		PTP：需要负载均衡

		PSB：不需要负载均衡
	*/
	//事实感觉分类完全多此一举,没必要啊,都是一模一样的代码
	switch req.option {
	//负载均衡
	case TOPIC_NIL_PTP_PUSH:
		{
			s.rmu.RLock()
			defer s.rmu.RUnlock()
			//先看这个是否订阅
			sub_name := GetStringFromSub(req.topic, req.partition, req.option)
			//第三个参数是consumer对应的broker接口，下面具体处理流程会回到这个s
			return s.topics[req.topic].StartToGetHandle(sub_name, req, s.consumers[req.consumer_ipname].GetToConsumer())
		}
		//广播
	case TOPIC_KEY_PSB_PUSH:
		{
			s.rmu.RLock()
			defer s.rmu.RUnlock()
			sub_name := GetStringFromSub(req.topic, req.partition, req.option)
			logger.DEBUG(logger.DLog, "consumer(%v) start to get topic(%v) partition(%v) offset(%v) in sub(%v)\n", req.consumer, req.topic, req.partition, req.offset, sub_name)
			return s.topics[req.topic].StartToGetHandle(sub_name, req, s.consumers[req.consumer_ipname].GetToConsumer())
		}
	default:
		err = errors.New("option error")

	}
	return err
}

// 检查topic和partition是否存在，不存在则需要创建他
// 设置partition的file和fd,start_index等信息
func (s *Server) PrepareAcceptHandle(in Info) (ret string, err error) {
	s.rmu.Lock()
	topic, ok := s.topics[in.topic]
	//创建一个新的topic
	if !ok {
		topic = NewTopic(s.name, in.topic)
		s.topics[in.topic] = topic
	}
	s.rmu.Unlock()
	return topic.prepareAcceptHandle(in)
}

// 停止接收文件，并将文件名修改成newfile,为什么要修改文件名
func (s *Server) CloseAcceptHandle(in Info) (start, end int64, ret string, err error) {
	s.rmu.RLock()
	topic, ok := s.topics[in.topic]
	if !ok {
		ret = "this topic is not in this broker"
		return 0, 0, ret, err
	}
	s.rmu.RUnlock()
	return topic.closeAcceptHandle(in)
}

// 准备发送信息，
// 检查topic和subscription是否存在，不存在则需要创建
// 检查该文件的config是否存在，不存在则创建，并开启协程
// 协程设置超时时间，时间到则关闭
func (s *Server) PrepareSendHandle(in Info) (ret string, err error) {
	s.rmu.Lock()
	topic, ok := s.topics[in.topic]
	//创建一个新的topic
	if !ok {
		topic = NewTopic(s.name, in.topic)
		s.topics[in.topic] = topic
	}
	s.rmu.Unlock()
	return topic.prepareSendHandle(in, &s.zkclient)
}
func (s *Server) PrepareStateHandle(in Info) (ret string, err error) {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	switch in.option {
	case -1:
		ok := s.parts_rafts.CheckPartState(in.topic, in.partition)
		if !ok {
			ret = "the raft not exits"
			err = errors.New(ret)
		}
	default:
	}
	return ret, err
}

// 给当前 Server 启动/加入某个 partition 的 Raft 群组
func (s *Server) AddRaftPartitionHandle(in Info) (ret string, err error) {
	logger.DEBUG(logger.DLog, "the raft brokers is %v\n", in.brokers)
	s.rmu.Lock()
	nodes := make(map[int]string)
	for k, v := range in.brok_me {
		nodes[v] = k
	}
	index := 0
	var peers []*raft_operations.Client
	for index < len(in.brokers) {
		logger.DEBUG(logger.DLog, "%v index (%v)  Me(%v)   k(%v) == Name(%v)\n", s.name, index, index, nodes[index], s.name)
		bro_cli, ok := s.brokers[nodes[index]]
		if !ok {
			logger.DEBUG(logger.DError, "%v new raft client fail err %v\n", s.name, err.Error())
			cli, err := raft_operations.NewClient(s.name, client.WithHostPorts(in.brokers[nodes[index]]))
			if err != nil {
				return ret, err
			}
			s.brokers[nodes[index]] = bro_cli
			bro_cli = &cli
		} else {
			logger.DEBUG(logger.DLog, "%v had client to broker %v\n", s.name, nodes[index])
		}
		peers = append(peers, bro_cli)
		index++
	}
	logger.DEBUG(logger.DLog, "the Broker %v raft Me %v\n", s.name, s.me)
	s.parts_rafts.AddPart_Raft(peers, s.me, in.topic, in.partition, s.aplych)
	s.rmu.Unlock()
	logger.DEBUG(logger.DLog, "the %v add over\n", s.name)
	return ret, err
}
func (s *Server) CloseRaftPartitionHandle(in Info) (ret string, err error) {
	s.rmu.Lock()
	err = s.parts_rafts.DeletePart_raft(in.topic, in.partition)
	s.rmu.Unlock()
	if err != nil {
		return err.Error(), err
	}
	return ret, err
}
func (s *Server) AddFetchPartitionHandle(in Info) (ret string, err error) {
	//检查该topic_partition是否准备好accept信息
	ret, err = s.PrepareAcceptHandle(in)
	if err != nil {
		logger.DEBUG(logger.DError, "%v err is %v\n", ret, err)
		return ret, err
	}
	if in.LeaderBroker == s.name {
		s.rmu.Lock()
		defer s.rmu.Unlock()
		topic, ok := s.topics[in.topic]
		if !ok {
			ret = "this topic is not this broker"
			logger.DEBUG(logger.DError, "%v, info(%v)\n", ret, in)
			return ret, errors.New(ret)
		}
		for BrokerName := range in.brokers {
			ret, err = topic.prepareSendHandle(Info{
				topic:     in.topic,
				partition: in.partition,
				file_name: in.file_name,
				consumer:  BrokerName,
				option:    TOPIC_KEY_PSB_PULL,
			}, &s.zkclient)
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}
		}
		return ret, err
	} else {
		time.Sleep(time.Microsecond * 100)
		str := in.topic + in.partition + in.file_name
		s.rmu.Lock()
		broker, ok := s.brokers_fetch[in.LeaderBroker]
		if !ok {
			bro_ptr, err := server_operations.NewClient(s.name, client.WithHostPorts(in.HostPort))
			if err != nil {
				return err.Error(), err
			}
			s.brokers_fetch[str] = &bro_ptr
			broker = &bro_ptr
		}

		topic, ok := s.topics[in.topic]
		if !ok {
			ret = "this topic is not this broker"
			return ret, err
		}
		s.rmu.Unlock()
		return s.FetchMsg(in, broker, topic)
	}
}
func (s *Server) FetchMsg(in Info, broker *server_operations.Client, topic *Topic) (ret string, err error) {
	//向zkserver请求向leader broker pull信息
	//向leader broker发起pull请求
	//获取本地当前文件end_index
	File, fd := topic.GetFile(in)
	index := File.GetIndex(fd)
	index += 1
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err.Error(), err
	}
	if in.file_name != "Nowfile.txt" {
		//当文件名不为nowfile时，创建partition,并向该File中写入内容
		go func() {
			Partition := NewPartition(s.name, in.topic, in.partition)
			Partition.StartGetMessage(File, fd, in)
			ice := 0
			for {
				resp, err := (*broker).Pull(context.Background(), &api.PullRequest{
					ConsumerId: s.name,
					Topic:      in.topic,
					Key:        in.partition,
					Offset:     index,
					Size:       10,
					Option:     TOPIC_KEY_PSB_PULL,
				})
				num := len(in.file_name)
				if err != nil {
					ice++
					logger.DEBUG(logger.DError, "Err %v, err(%v)\n", resp, err.Error())
					if ice >= 3 {
						time.Sleep(time.Second * 3)
						resp, err := s.zkclient.GetNewLeader(context.Background(), &api.GetNewLeaderRequest{
							Topic:     in.topic,
							Part:      in.partition,
							BlockName: in.file_name[:num-4],
						})
						if err != nil {
							logger.DEBUG(logger.DError, "%v\n", err.Error())
						}
						s.rmu.Lock()
						_, ok := s.brokers_fetch[in.topic+in.partition]
						if !ok {
							logger.DEBUG(logger.DLog, "this broker(%v) is not connected\n", s.name)
							leader_bro, err := server_operations.NewClient(s.name, client.WithHostPorts(resp.HostPort))
							if err != nil {
								logger.DEBUG(logger.DError, "%v\n", err.Error())
								return
							}
							s.brokers_fetch[resp.LeaderBroker] = &leader_bro
							broker = &leader_bro
						}
						s.rmu.Unlock()
					}
					continue
				}
				if resp.Err == "file EOF" {
					logger.DEBUG(logger.DLog, "This Partition(%d) filename(%d) is over\n", in.partition, in.file_name)
					fd.Close()
					return
				}
				ice = 0
				if resp.StartIndex <= index && resp.EndIndex > index {
					//index 处于返回包的中间位置
					//需要截断该宝包，并写入
					//your code
					logger.DEBUG(logger.DLog, "need your code\n")
				}
				node := NodeData{
					Start_index: resp.StartIndex,
					End_index:   resp.EndIndex,
					Size:        len(resp.Msgs),
				}
				File.WriteFile(fd, node, resp.Msgs)
				index = resp.EndIndex + 1
				s.zkclient.UpdateDup(context.Background(), &api.UpdateDupRequest{
					Topic:      in.topic,
					Part:       in.partition,
					BlockName:  GetBlockName(in.file_name),
					BrokerName: s.name,
					EndIndex:   node.End_index,
				})
			}
		}()
	} else {
		go func() {
			fd.Close()
			s.rmu.RLock()
			topic, ok := s.topics[in.topic]
			s.rmu.RUnlock()
			if !ok {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}

			ice := 0
			for {
				resp, err := (*broker).Pull(context.Background(), &api.PullRequest{
					ConsumerId: s.name,
					Topic:      in.topic,
					Key:        in.partition,
					Offset:     index,
					Size:       10,
					Option:     TOPIC_KEY_PSB_PULL,
				})

				if err != nil {
					ice++
					logger.DEBUG(logger.DError, "Err %v, err(%v)\n", resp, err.Error())
					if ice >= 3 {
						time.Sleep(time.Second * 3)
						resp, err := s.zkclient.GetNewLeader(context.Background(), &api.GetNewLeaderRequest{
							Topic:     in.topic,
							Part:      in.partition,
							BlockName: "NowBlock",
						})
						if err != nil {
							logger.DEBUG(logger.DError, "%v\n", err.Error())
						}
						s.rmu.Lock()
						_, ok := s.brokers_fetch[in.topic+in.partition]
						if !ok {
							logger.DEBUG(logger.DLog, "this broker(%v) is not connected\n", s.name)
							leader_bro, err := server_operations.NewClient(s.name, client.WithHostPorts(resp.HostPort))
							if err != nil {
								logger.DEBUG(logger.DError, "%v\n", err.Error())
								return
							}
							s.brokers_fetch[resp.LeaderBroker] = &leader_bro
							broker = &leader_bro
						}
						s.rmu.Unlock()
					}
					continue
				}
				if resp.Size == 0 {
					time.Sleep(time.Second * 10)
				} else {
					msgs := make([]Message, resp.Size)
					json.Unmarshal(resp.Msgs, &msgs)
					start_index := resp.StartIndex
					for _, msg := range msgs {
						if index == start_index {
							err := topic.AddMessage(Info{
								topic:      in.topic,
								partition:  in.partition,
								file_name:  "NowBlock.txt",
								zkclient:   &s.zkclient,
								message:    msg.Msg,
								size:       msg.Size,
								BrokerName: s.name,
							})
							if err != nil {
								logger.DEBUG(logger.DError, "%v\n", err.Error())
							}
						}
						index++
					}
				}
			}
		}()
	}
	return ret, err
}
func (s *Server) CloseFetchPartitionHandle(in Info) (ret string, err error) {
	str := in.topic + in.partition + in.file_name
	s.rmu.Lock()
	defer s.rmu.Unlock()
	_, ok := s.parts_fetch[str]
	if !ok {
		ret := "this topic-partition is not in this brpoker"
		logger.DEBUG(logger.DError, "this topic(%v)-partition(%v) is not in this brpoker\n", in.topic, in.partition)
		return ret, errors.New(ret)
	} else {
		delete(s.parts_fetch, str)
		return ret, err
	}
}

// 感觉暂时不需要这个了,因为现在把他变到zkserver了
type SubResponse struct {
	size  int
	parts []PartName
}

// 现在完全看不到是在那里调用了他
// 订阅这个动作无论是加入还是取消都与topic结构体和Consumer结构体有关，他们两个都要操作
// 通过Sub结构体来订阅消息
func (s *Server) Sub2Handle(in Info) (err error) {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	//这里还得先判断一下这个topic有没有
	topic, ok := s.topics[in.topic]
	if !ok {
		topic = NewTopic(s.name, in.topic)
		return errors.New("topic not exist")
	}
	sub, err := topic.AddScription(in)
	if err != nil {
		return err
	}
	s.consumers[in.consumer].AddScription(sub)
	return nil
}

func (s *Server) UnSubHandle(in Info) error {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	//这里还得先判断一下这个topic有没有
	topic, ok := s.topics[in.topic]
	if !ok {
		return errors.New("topic not exist")
	}
	//这里消费者要删除这个订阅就把这个订阅全部删除了吗？感觉好奇怪
	sub_name, err := topic.ReduceScription(in)
	if err != nil {
		return err
	}
	s.consumers[in.consumer].ReduceScription(sub_name)
	return nil
}
