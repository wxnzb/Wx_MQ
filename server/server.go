package server

import (
	"Wx_MQ/kitex_gen/api/client_operations"

	"sync"

	cl "github.com/cloudwego/kitex/client"
)

type Server struct {
	topics map[string]*Topic
	//groups map[string]Group
	consumers map[string]*Consumer
	rmu       sync.RWMutex
}
type Sub struct {
	consumer string
	topic    string
	key      string
	option   int8
}

func (s *Server) make() {
	s.topics = make(map[string]*Topic)
	s.consumers = make(map[string]*Consumer)
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
func (s *Server) InfoHandle(ip_port string) error {
	client, err := client_operations.NewClient("client", cl.WithHostPorts(ip_port))
	if err == nil {
		//现在这样写，等于是全部的消费者都加入到了s的一个消费组里面
		//s.groups["default"].consumers[ip_port] = &client
		s.rmu.Lock()
		consumer, ok := s.consumers[ip_port]
		if !ok {
			consumer = NewConsumer(ip_port, client)
			s.consumers[ip_port] = consumer
		}
		go s.CheckConsumer(consumer)
		s.rmu.Unlock()
		return nil
	}
	return err
}
func (s *Server) CheckConsumer(consumer *Consumer) {
	shutDown := consumer.CheckConsumer()
	if shutDown {
		consumer.rmu.Lock()
		for _, subscription := range consumer.subList {
			subscription.shutDownConsumer(consumer.name)
		}
		consumer.rmu.Unlock()
	}
}

type Push struct {
	producerId int64
	topic      string
	key        string
	message    string
}

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

type PullRequest struct {
	consumerId int64
	topic      string
	key        string
}
type PullResponse struct {
	message string
}

func (s *Server) PullHandle(pullRequest PullRequest) (PullResponse, error) {
	return PullResponse{message: "haha"}, nil
}
func (s *Server) SubHandle(req Sub) error {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	sub, err := s.topics[req.topic].AddScription(req)
	if err != nil {
		return err
	}
	s.consumers[req.consumer].AddScription(sub)
	return nil
}
