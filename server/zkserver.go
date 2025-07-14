package server

import (
	"Wx_MQ/kitex_gen/api/server_operations"
	"Wx_MQ/zookeeper"
	"sync"

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

}

type Info_in struct {
	TopicName     string
	PartitionName string
	Option        int8
	CliName       string
	Index         int64
}
type Info_out struct {
	Err           error
	bro_host_port string
}

func (zks *ZKServer) ProGetBroHandle(block Info_in) Info_out {

}
func (zks *ZKServer) ConGetBroHandle(block Info_in) Info_out {

}
func (zks *ZKServer) SubHandle(sub SubRequest) error {

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
