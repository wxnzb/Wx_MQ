package zookeeper

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/go-zookeeper/zk"
)

type ZK struct {
	Con        *zk.Conn
	Root       string
	BrokerRoot string
	TopicRoot  string
}
type ZKInfo struct {
	host_post      []string
	root           string
	sessionTimeout time.Duration
}

func NewZK(info ZKInfo) *ZK {
	conn, _, err := zk.Connect(info.host_post, info.sessionTimeout)
	if err != nil {
		fmt.Println("connect zookeeper error:", err)
	}
	return &ZK{
		Con:        conn,
		Root:       info.root,
		BrokerRoot: info.root + "/Brokers",
		TopicRoot:  info.root + "/Topics",
	}
}

type BrokerNode struct {
	Name string `json:"name"`
	Host string `json:"host"`
	Port string `json:"port"`
}
type TopicNode struct {
	Name    string   `json:"name"`
	Brokers []string `json:"brokers"`
}
type PartitionNode struct {
	Name  string `json:"name"`
	Topic string `json:"topic"`
}
type BlockNode struct {
	Name        string `json:"name"`
	Topic       string `json:"topic"`
	Partition   string `json:"partition"`
	StartOffset int64  `json:"start_offset"`
	EndOffset   int64  `json:"end_offset"`
}

// 使用反射确定节点类型
func (z *ZK) RegisterNode(node interface{}) error {
	path := ""
	var bronode BrokerNode
	var tNode TopicNode
	var pNode PartitionNode
	var bloNode BlockNode
	i := reflect.TypeOf(node)
	var data []byte
	switch i.Name() {
	case "BrokerNode":
		bronode = node.(BrokerNode)
		path = z.BrokerRoot + "/" + bronode.Name
		data, _ = json.Marshal(bronode)
	case "TopicNode":
		tNode = node.(TopicNode)
		path = z.TopicRoot + "/" + tNode.Name
		data, _ = json.Marshal(tNode)
	case "PartitionNode":
		pNode = node.(PartitionNode)
		path = z.TopicRoot + "/" + pNode.Topic + pNode.Name
		data, _ = json.Marshal(pNode)
	case "BlockNode":
		bloNode = node.(BlockNode)
		path = z.TopicRoot + "/" + bloNode.Topic + "/" + bloNode.Partition + "/" + bloNode.Name
		data, _ = json.Marshal(bloNode)
	}
	_, err := z.Con.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}

// 获取Topic的Broker信息
func (z *ZK) GetBrokers(Topic string) ([]string, error) {
	path := z.TopicRoot + "/" + Topic
	exists, _ := z.Con.Exists(path)
	if !exists {
		return nil, err
	}
	data, _ := z.Con.Get(path)
	var tNode TopicNode
	err := json.Unmarshal(data, &tNode)
	if err != nil {
		return nil, err
	}
	return tNode.Brokers, nil
}
