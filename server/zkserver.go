package server

import (
	"Wx_MQ/kitex_gen/api/server_operations"
	"Wx_MQ/zookeeper"
	"sync"
)

type ZKServer struct {
	rmu sync.RWMutex
	zk  zookeeper.ZK

	info_brokers   map[string]zookeeper.BrokerNode
	info_topics    map[string]zookeeper.TopicNode
	info_partition map[string]zookeeper.PartitionNode

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

}

type UnknowName_in struct {
	TopicName     string
	PartitionName string
}
type UnknowName_out struct {
	err error
}

func (zks *ZKServer) CreateTopic(topic UnknowName_in) UnknowName_out {
	tNode := zookeeper.TopicNode{
		Name: topic.TopicName,
	}
	err := zks.zk.RegisterNode(tNode)
	return UnknowName_out{
		err: err,
	}
}
func (zks *ZKServer) CreatePartition(part UnknowName_in) UnknowName_out {
	pNode := zookeeper.PartitionNode{
		Name:     part.PartitionName,
		Topic:    part.TopicName,
		PTPIndex: int64(0),
	}
	err := zks.zk.RegisterNode(pNode)
	err = zks.CreateNowBlock(part)
	return UnknowName_out{
		err: err,
	}
}
func (zks *ZKServer) CreateNowBlock(block UnknowName_in) error {
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
