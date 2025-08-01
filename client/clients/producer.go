package clients

import (
	"Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/server_operations"
	"Wx_MQ/kitex_gen/api/zkserver_operations"
	"context"
	"errors"
	"sync"

	"github.com/cloudwego/kitex/client"
)

type Producer struct {
	Cli             server_operations.Client //生产者需要调用的接口
	rmu             sync.RWMutex
	Name            string
	Topic_Partition map[string]server_operations.Client //这个topic的Partition是否是这个生产者负责
	zkBrokerCli     zkserver_operations.Client
}

func NewProducer(zkBrokerIpport, name string) (*Producer, error) {
	pro := Producer{
		rmu:             sync.RWMutex{},
		Name:            name,
		Topic_Partition: make(map[string]server_operations.Client),
	}
	var err error
	pro.zkBrokerCli, err = zkserver_operations.NewClient(name, client.WithHostPorts(zkBrokerIpport))
	return &pro, err
}

type Message struct {
	Topic_Name     string
	Partition_Name string
	Msg            string
}

func (pro *Producer) Push(msg Message) error {
	pro.rmu.RLock()
	index := msg.Topic_Name + "_" + msg.Partition_Name
	cli, ok := pro.Topic_Partition[index]
	pro.rmu.RUnlock()
	if !ok {
		//要是找不到，就说明这个topic-partition的broker还没有连接到生产者，先要进行连接
		resp, err := pro.zkBrokerCli.ProGetBro(context.Background(), &api.ProGetBroRequest{
			TopicName:     msg.Topic_Name,
			PartitionName: msg.Partition_Name,
		})
		if err != nil || !resp.Ret {
			return err
		}
		cli, err := server_operations.NewClient(pro.Name, client.WithHostPorts(resp.BroHostPort))
		if err != nil {
			return err
		}
		pro.rmu.RLock()
		pro.Topic_Partition[index] = cli
		pro.rmu.RUnlock()
	}
	resp, err := cli.Push(context.Background(), &api.PushRequest{
		ProducerId: pro.Name,
		Topic:      msg.Topic_Name,
		Key:        msg.Partition_Name,
		Message:    msg.Msg,
	})
	if err == nil && resp.Ret {
		return nil
		//我不再处理你这个 Partition 了
		//这里需要在进行理解
	} else if resp.Err == "partition remove" {
		pro.rmu.Lock()
		delete(pro.Topic_Partition, index)
		pro.rmu.Unlock()
		return pro.Push(msg)
	} else {
		return errors.New("err!=nil or resp.Ret==false\n")
	}
}
func (pro *Producer) CreateTopic(topic string) error {
	resp, err := pro.zkBrokerCli.CreateTopic(context.Background(), &api.CreateTopicRequest{
		TopicName: topic,
	})
	if !resp.Ret || err != nil {
		return err
	}
	return nil
}
func (pro *Producer) CreateTopicPartition(topic, partition string) error {
	resp, err := pro.zkBrokerCli.CreatePartition(context.Background(), &api.CreatePartitionRequest{
		TopicName:     topic,
		PartitionName: partition,
	})
	if !resp.Ret || err != nil {
		return err
	}
	return nil
}
