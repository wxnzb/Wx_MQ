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
	}
}
func (zs *ZKServer) make(opt Options) {

}
