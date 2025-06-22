package server

import (
	"Wx_MQ/kitex_gen/api/client_operations"

	"sync"

	"errors"

	cl "github.com/cloudwego/kitex/client"
)

type Server struct {
	topics    map[string]*Topic
	consumers map[string]*ToConsumer //这里的string是消费者的ip_port
	rmu       sync.RWMutex
}

func (s *Server) make() {
	s.topics = make(map[string]*Topic)
	s.consumers = make(map[string]*ToConsumer)
	s.rmu = sync.RWMutex{}
	s.StartRelease()
}
func (s *Server) StartRelease() {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	for _, topic := range s.topics {
		go topic.StartRelease(s) //先时获得所有的topic，然后获得所有topic里面的partition,然后获得所有partition里面的所有消费者，通过这个消费者获得*Client
	}
}

// 将消费者客户端连接到服务器
func (s *Server) InfoHandle(ip_port string) error {
	//消费者客户端
	client, err := client_operations.NewClient("client", cl.WithHostPorts(ip_port))
	if err == nil {
		//现在这样写，等于是全部的消费者都加入到了s的一个消费组里面
		//s.groups["default"].consumers[ip_port] = &client
		s.rmu.Lock()
		consumer, ok := s.consumers[ip_port]
		if !ok {
			consumer = NewToConsumer(ip_port, client)
			s.consumers[ip_port] = consumer
		}
		go s.CheckConsumer(consumer)
		go s.RecoverConsumer(consumer)
		s.rmu.Unlock()
		return nil
	}
	return err
}

// 循环遍历这个消费者是否还在先，要是没在先，就要将他所有的订阅都删除调并重新进行平衡
func (s *Server) CheckConsumer(consumer *ToConsumer) {
	shutDown := consumer.CheckConsumer()
	if shutDown {
		consumer.rmu.Lock()
		for _, subscription := range consumer.subList {
			topic_name := subscription.shutDownConsumer(consumer.name)
			//我现在感觉s.topics[subscription.topic_name].Rebalance()这样写不是更简单吗,还不用给shutDownConsumer这个函数加返回值，恩呢，感觉确实可以
			s.topics[topic_name].Rebalance()
		}
		consumer.rmu.Unlock()
	}
}

// 上面的要是失败了死了就取消，现在又要将他变成活得
func (s *Server) RecoverConsumer(consumer *ToConsumer) {
	s.rmu.Lock()
	consumer.rmu.Lock()
	consumer.state = ALIVE
	for sub_name, sub := range consumer.subList {
		go s.topics[sub.topic_name].RecoverConsumer(sub_name, consumer)
	}
	consumer.rmu.Unlock()
	s.rmu.Unlock()
}

type Push struct {
	producerId string
	topic      string
	key        string
	message    string
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
	topic.AddMessage(s, push)
	return nil
}
func (s *Server) AddMessage(topic *Topic, req Push) {
	part, ok := topic.Parts[req.key]
	if ok {
		part.rmu.Lock()
		part.queue = append(part.queue, req.message)
		part.rmu.Unlock()
	} else {
		part = NewPartition(req)
		go part.Release(s)
		topic.Parts[req.key] = part
	}
}

type PullRequest struct {
	consumerId string
	topic      string
	key        string
}
type PullResponse struct {
	message string
}

func (s *Server) PullHandle(pullRequest PullRequest) (PullResponse, error) {
	return PullResponse{message: "haha"}, nil
}

type Sub struct {
	consumer string
	topic    string
	key      string
	option   int8
}

// 订阅这个动作无论是加入还是取消都与topic结构体和Consumer结构体有关，他们两个都要操作
// 通过Sub结构体来订阅消息
func (s *Server) SubHandle(req Sub) error {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	//这里还得先判断一下这个topic有没有
	topic, ok := s.topics[req.topic]
	if !ok {
		return errors.New("topic not exist")
	}
	//sub, err := s.topics[req.topic].AddScription(req)
	sub, err := topic.AddScription(req, s.consumers[req.consumer])
	if err != nil {
		return err
	}
	s.consumers[req.consumer].AddScription(sub)
	return nil
}
func (s *Server) UnSubHandle(req Sub) error {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	//这里还得先判断一下这个topic有没有
	topic, ok := s.topics[req.topic]
	if !ok {
		return errors.New("topic not exist")
	}
	//这里消费者要删除这个订阅就把这个订阅全部删除了吗？感觉好奇怪
	sub_name, err := topic.ReduceScription(req)
	if err != nil {
		return err
	}
	s.consumers[req.consumer].ReduceScription(sub_name)
	return nil
}

type PartitionInitInfo struct {
	topic           string
	partition       string
	consumer_ipname string
	option          int8
	index           int64
}

// 这个还没有实现
func (s *Server) StartGet(req PartitionInitInfo) error {
	return nil
}

const (
	NODE_SIZE = 42
)

type NodeData struct {
	Start_index int64 `json:"start_index"` //这几批消息的第一批消息的index
	End_index   int64 `json:"end_index"`   //这几批消息的最后一批消息的index
	Size        int   `json:"size"`        //这几批消息的总大小
}
type Message struct {
	Topic_name     string `json:topic_name`
	Partition_name string `json:partition_name`
	Index          int64  `json:"index"`
	Msgs           []byte `json:msgs`
}
