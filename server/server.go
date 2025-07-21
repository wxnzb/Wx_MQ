package server

import (
	"encoding/json"
	"os"
	"sync"

	"errors"
	//cl "github.com/cloudwego/kitex/client"
	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/zkserver_operations"
	"Wx_MQ/zookeeper"

	//"github.com/docker/docker/client"
	"context"

	"github.com/docker/docker/client"
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
}
type Msgs struct {
	producer string
	topic    string
	key      string
	msgs     []byte
}

// -------------------------------
type Server struct {
	topics    map[string]*Topic
	consumers map[string]*ToConsumer //这里的string是消费者的ip_port
	rmu       sync.RWMutex
	zkclient  zkserver_operations.Client
	zk        zookeeper.ZK
	name      string
}

var ip_name string //加了这个
// 其实感觉挺奇怪的，这里为什么不直接把这个放到make里面呢？？？？
func NewServer(zkInfo zookeeper.ZKInfo) *Server {
	return &Server{
		rmu: sync.RWMutex{},
		zk:  *zookeeper.NewZK(zkInfo),
	}
}

// 这个broker先和zk建立联系，将这个broker像zk进行注册
func (s *Server) make(opt Options) {
	s.topics = make(map[string]*Topic)
	s.consumers = make(map[string]*ToConsumer)
	ip_name = GetIpPort()
	s.CheckList()
	s.name = opt.Name
	s.zkclient, _ = zkserver_operations.NewClient(opt.Name, client.WithHost(opt.ZKServerHostPort))
	resp, err := s.zkclient.BroInfo(context.Background(), &api.BroInfoRequest{
		BroName:     opt.Name,
		BroHostPort: opt.BrokerHostPort,
	})
	if resp.Ret == false || err != nil {
		DEBUG(dERROR, "broker register failed")
	}
	s.InitBroker()
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
	for topic_name, top := range topics {
		_, ok := s.topics[topic_name]
		if !ok {
			s.HandlePartitions(topic_name, top.Partitions)
		} else {
			DEBUG(dWARN, "This topic(%v) had in s.topics\n", topic_name)
		}
	}
}
func (s *Server) HandlePartitions(topic_name string, partitions map[string]Part_Info) {
	for partition_name, part := range partitions {
		_, ok := s.topics[topic_name].Parts[partition_name]
		if !ok {
			s.topics[topic_name] = NewTopic(topic_name)
			s.HandleBlocks(topic_name, partition_name, part.Blocks)
		} else {
			DEBUG(dWARN, "This partition(%v) had in s.topics[%v].Parts\n", partition_name, topic_name)
		}
	}
}

// 这个还没有实现
func (s *Server) HandleBlocks(topic_name, partition_name string, blocks_info map[string]Block_Info) {

}

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
type Push struct {
	producerId string
	topic      string
	key        string
	message    string
	option     int8
}

// 服务器将消息存在topic里面
func (s *Server) PushHandle(push Push) error {
	topic, ok := s.topics[push.topic]
	if !ok {
		//创建一个新的topic
		topic := NewTopic(push)
		s.rmu.Lock()
		s.topics[push.topic] = topic
		s.rmu.Unlock()
	}
	topic.AddMessage(push)
	return nil
}

// 2
type PullRequest struct {
	consumerId string
	topic      string
	key        string
}
type PullResponse struct {
	message string
}

func (s *Server) PullHandle(pullRequest PullRequest) (PullResponse, error) {
	return PullResponse{message: ""}, nil
}

// 3
func (s *Server) InfoHandle(ip_port string) (err error) {
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
	//go s.RecoverConsumer(consumer)
	s.rmu.Unlock()
	return nil
}

// 4
// name       string //broker name
// 	topic_name string
// 	part_name  string
// 	file_name  string
// 	option     int8
// 	offset     int64

// producer string
// consumer string
type PartitionInfo struct {
	name            string
	topic           string
	partition       string
	consumer_ipname string
	option          int8
	offset          int64
	file_name       string
	producer        string
	consumer        string
}

func (s *Server) StartGet(req PartitionInfo) (err error) {
	//先看
	switch req.option {
	//负载均衡
	case TOPIC_NIL_PTP:
		{
			s.rmu.Lock()
			defer s.rmu.Unlock()
			//先看这个是否订阅
			sub_name := GetStringFromSub(req.topic, req.partition, req.option)
			ret := s.consumers[req.consumer_ipname].CheckSubscription(sub_name)
			sub := s.consumers[req.consumer_ipname].GetSub(sub_name)
			if ret == true {
				sub.AddConsumerInConfig(req, s.consumers[req.consumer_ipname].GetToConsumer())
			} else {
				err = errors.New("not subscribe")
			}
		}
		//广播
	case TOPIC_KEY_PSB:
		{
			s.rmu.Lock()
			defer s.rmu.Unlock()
			//先看这个是否订阅
			sub_name := GetStringFromSub(req.topic, req.partition, req.option)
			ret := s.consumers[req.consumer_ipname].CheckSubscription(sub_name)
			if ret == true {
				//下面这三行是为了没有这个分区的时候新建一个
				// toConsumers := make(map[string]*client_operations.Client)
				// toConsumers[req.consumer_ipname] = s.consumers[req.consumer_ipname].GetToConsumer()
				// file := s.topics[req.topic].GetFile(req.partition)
				// go s.consumers[req.consumer_ipname].StartPart(req, toConsumers, file)
			} else {
				err = errors.New("not subscribe")
			}
		}
	default:
		err = errors.New("option error")

	}
	return err
}

// 5所以他也把文件名传进来有什么用呢
func (s *Server) PrepareAcceptHandle(pinfo PartitionInfo) (ret string, err error) {
	s.rmu.Lock()
	defer s.rmu.Unlock()
    topic,ok:=s.topics[pinfo.topic]
	//创建一个新的topic
	if !ok{
        topic=NewTopic(pinfo.topic)
		s.topics[pinfo.topic]=topic
	}
	topic.PrepareAcceptHandle(pinfo)
}

// 6
func (s *Server) PrepareSendHandle(pinfo PartitionInfo) (ret string, err error) {

}

// 感觉暂时不需要这个了,因为现在把他变到zkserver了
type SubRequest struct {
	consumer string
	topic    string
	key      string
	option   int8
}
type SubResponse struct {
	size  int
	parts []PartName
}

// // 订阅这个动作无论是加入还是取消都与topic结构体和Consumer结构体有关，他们两个都要操作
// // 通过Sub结构体来订阅消息
// func (s *Server) SubHandle(req SubRequest) (err error) {
// 	s.rmu.Lock()
// 	defer s.rmu.Unlock()
// 	DEBUG(dLOG, "get sub information")
// 	//这里还得先判断一下这个topic有没有
// 	topic, ok := s.topics[req.topic]
// 	if !ok {
// 		return errors.New("topic not exist")
// 	}
// 	sub, err := topic.AddScription(req, s.consumers[req.consumer])
// 	if err != nil {
// 		return err
// 	}
// 	s.consumers[req.consumer].AddScription(sub)
// 	return nil
// }

// func (s *Server) UnSubHandle(req SubRequest) error {
// 	s.rmu.Lock()
// 	defer s.rmu.Unlock()
// 	//这里还得先判断一下这个topic有没有
// 	topic, ok := s.topics[req.topic]
// 	if !ok {
// 		return errors.New("topic not exist")
// 	}
// 	//这里消费者要删除这个订阅就把这个订阅全部删除了吗？感觉好奇怪
// 	sub_name, err := topic.ReduceScription(req)
// 	if err != nil {
// 		return err
// 	}
// 	s.consumers[req.consumer].ReduceScription(sub_name)
// 	return nil
// }
