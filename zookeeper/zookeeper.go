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
	Name string `json:"name"`
	//Brokers []string `json:"brokers"`
}
type PartitionNode struct {
	Name     string `json:"name"`
	Topic    string `json:"topic"`
	PTPIndex int64  `json:"ptpindex"`
}
type BlockNode struct {
	Name        string `json:"name"`
	Topic       string `json:"topic"`
	Partition   string `json:"partition"`
	StartOffset int64  `json:"start_offset"`
	EndOffset   int64  `json:"end_offset"`
	FileName    string `json:filename`
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

type Part struct {
	topicName     string
	partitionName string
	brokerName    string
	broHostPort   string
	PTPIndex      int64 //名字解释：生产端写入某个 Topic/Partition 的偏移
	FileName      string
}

// 获取Topic下所有partition对应的的Broker信息
func (z *ZK) GetBrokers(Topic string) ([]Part, error) {
	path := z.TopicRoot + "/" + Topic + "partition"
	exists, _, err := z.Con.Exists(path)
	if !exists {
		return nil, err
	}
	// data, _, _ := z.Con.Get(path)
	// var tNode TopicNode
	// err = json.Unmarshal(data, &tNode)
	// if err != nil {
	// 	return nil, err
	// }
	// return tNode.Brokers, nil
	var parts []Part
	partitions, _, _ := z.Con.Children(path)
	for _, partition := range partitions {
		//得到这个分区正在写入的位置和他所有的blocks
		PTPIndex := z.GetPartitionPTPIndex(path + "/" + partition)
		blocks, _, _ := z.Con.Children(path + "/" + partition)
		for _, block := range blocks {
			blockInfo := z.GetBlockNode(path + "/" + partition + "/" + block)
			//那么就说明这个block是part正在写入的块
			if blockInfo.StartOffset <= PTPIndex && blockInfo.EndOffset >= PTPIndex {
				brokerInfo := z.GetBrokerNode(blockInfo.Name)
				parts = append(parts, Part{
					topicName:     Topic,
					partitionName: partition,
					brokerName:    brokerInfo.Name,
					broHostPort:   brokerInfo.Host + ":" + brokerInfo.Port,
					PTPIndex:      PTPIndex,
					FileName:      blockInfo.FileName,
				})
			}
		}
	}
	return parts, nil
}
func (z *ZK) GetPartitionPTPIndex(path string) int64 {
	var pNode PartitionNode
	data, _, _ := z.Con.Get(path)
	json.Unmarshal(data, &pNode)
	return pNode.PTPIndex
}
func (z *ZK) GetPartitionNode(path string) (PartitionNode, error) {
	var pnode PartitionNode
	ok, _, err := z.Con.Exists(path)
	if !ok {
		return pnode, err
	}
	data, _, _ := z.Con.Get(path)
	json.Unmarshal(data, &pnode)
	return pnode, nil
}
func (z *ZK) GetBlockNode(path string) BlockNode {
	var bloNode BlockNode
	data, _, _ := z.Con.Get(path)
	json.Unmarshal(data, &bloNode)
	return bloNode
}
func (z *ZK) GetBrokerNode(path string) BrokerNode {
	//这个为什么path和上面的不一样
	path = z.BrokerRoot + "/" + path
	var broNode BrokerNode
	data, _, _ := z.Con.Get(path)
	json.Unmarshal(data, &broNode)
	return broNode
}
func (z *ZK) UpdatePartitionNode(pnode PartitionNode) error {
	path := z.TopicRoot + "/" + pnode.Topic + "/" + "Partitions/" + pnode.Name
	data, err := json.Marshal(pnode)
	if err != nil {
		return err
	}
	_, sate, _ := z.Con.Get(path)
	_, err = z.Con.Set(path, data, sate.Version)
	if err != nil {
		return err
	}
	return nil
}
