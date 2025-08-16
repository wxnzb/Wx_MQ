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
	Name         string `json:"name"`
	BroHostPort  string `json:"brohostport"`
	RaftHostPort string `json:"rafthostport"`
	Me           int    `json:"me"`
	Pnum         int    `json:"pnum"`
}
type TopicNode struct {
	Name string `json:"name"`
	//Brokers []string `json:"brokers"`
}
type PartitionNode struct {
	Name     string `json:"name"`
	Topic    string `json:"topic"`
	PTPIndex int64  `json:"ptpindex"`
	Option   int8   `json:option`
	Index    int64  `json:index`
	DupNum   int8   `json:dupnum`
}
type BlockNode struct {
	Name         string `json:"name"`
	Topic        string `json:"topic"`
	Partition    string `json:"partition"`
	StartOffset  int64  `json:"start_offset"`
	EndOffset    int64  `json:"end_offset"`
	FileName     string `json:filename`
	LeaderBroker string `json:leaderbroker`
}
type DuplicateNode struct {
	Name        string `json:"name"`
	Topic       string `json:"topic"`
	Partition   string `json:"partition"`
	StartOffset int64  `json:"start_offset"`
	EndOffset   int64  `json:"end_offset"`
	BrokerName  string `json:"brokername"`
	BlockName   string `json:"blockname"`
}

// 在 Zookeeper 指定路径下创建一个永久节点，并且把给定的结构体内容（如 BlockNode、BrokerNode 等）转成 JSON 后写入该节点中。
func (z *ZK) RegisterNode(node interface{}) (err error) {
	path := ""
	var bronode BrokerNode
	var tNode TopicNode
	var pNode PartitionNode
	var bloNode BlockNode
	//reflect.TypeOf(node) 反射的作用是获取 node 的类型信息
	i := reflect.TypeOf(node)
	var data []byte
	switch i.Name() {
	case "BrokerNode":
		bronode = node.(BrokerNode)
		path = z.BrokerRoot + "/" + bronode.Name
		data, err = json.Marshal(bronode)
	case "TopicNode":
		tNode = node.(TopicNode)
		path = z.TopicRoot + "/" + tNode.Name
		data, err = json.Marshal(tNode)
	case "PartitionNode":
		pNode = node.(PartitionNode)
		path = z.TopicRoot + "/" + pNode.Topic + pNode.Name
		data, err = json.Marshal(pNode)
	case "BlockNode":
		bloNode = node.(BlockNode)
		path = z.TopicRoot + "/" + bloNode.Topic + "/" + bloNode.Partition + "/" + bloNode.Name
		data, err = json.Marshal(bloNode)
	}
	if err != nil {
		return err
	}
	_, err = z.Con.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}

type Part struct {
	topicName     string
	PartitionName string
	BrokerName    string
	BroHostPort   string
	PTPIndex      int64 //名字解释：生产端写入某个 Topic/Partition 的偏移
	FileName      string
	Err           string
}

// 获取Topic下所有partition对应的消费到的block信息
func (z *ZK) GetBrokers(Topic string) ([]Part, error) {
	path := z.TopicRoot + "/" + Topic + "partition"
	exists, _, err := z.Con.Exists(path)
	if !exists {
		return nil, err
	}
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
					PartitionName: partition,
					BrokerName:    brokerInfo.Name,
					BroHostPort:   brokerInfo.Host + ":" + brokerInfo.Port,
					PTPIndex:      PTPIndex,
					FileName:      blockInfo.FileName,
				})
			}
		}
	}
	return parts, nil
}

// 获取Topic下特定partition对应的消费者需要的offset的block信息
func (z *ZK) GetBroker(topic, part string, offfset int64) ([]Part, error) {
	path := z.TopicRoot + "/" + topic + "/partitions/" + part
	exists, _, err := z.Con.Exists(path)
	if !exists {
		return nil, err
	}
	var parts []Part
	blocks, _, _ := z.Con.Children(path)
	for _, block := range blocks {
		blockInfo, _ := z.GetBlockNode(path + "/" + part + "/" + block)
		//找到消费者需要的offset的block
		if blockInfo.StartOffset <= offfset && blockInfo.EndOffset >= offfset {
			brokerInfo, _ := z.GetBrokerNode(blockInfo.Name)
			parts = append(parts, Part{
				topicName:     topic,
				PartitionName: part,
				BrokerName:    brokerInfo.Name,
				BroHostPort:   brokerInfo.Host + ":" + brokerInfo.Port,
				FileName:      blockInfo.FileName,
			})
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
func (z *ZK) GetPartitionNode(topic, part string) (PartitionNode, error) {
	path := z.TopicRoot + "/" + topic + "/Partitions/" + part
	var pnode PartitionNode
	ok, _, err := z.Con.Exists(path)
	if !ok {
		return pnode, err
	}
	data, _, _ := z.Con.Get(path)
	json.Unmarshal(data, &pnode)
	return pnode, nil
}

// 这个更新的时候只需要变化的成员还是结构体整个都要写上
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
func (z *ZK) GetNowPartBrokerNode(topic_name, part_name string) (BrokerNode, BlockNode, int8, error) {
	Now_block_path := z.TopicRoot + "/" + topic_name + "/Partitions/" + part_name + "/" + "NowBlock"
	for {
		NowBlock, err := z.GetBlockNode(Now_block_path)
		if err != nil {
			return BrokerNode{}, BlockNode{}, 0, err
		}
		NowBroker, err := z.GetBrokerNode(NowBlock.LeaderBroker)
		if err != nil {
			return BrokerNode{}, BlockNode{}, 0, err
		}
		ret := z.CheckBroker(z.BrokerRoot + NowBlock.LeaderBroker)
		if ret {
			return NowBroker, NowBlock, 2, nil
		} else {
			time.Sleep(time.Second * 1)
		}
	}
}
func (z *ZK) GetDuplicateNodes(topic_name, part_name, block_name string) (nodes []DuplicateNode) {
	BlockPath := z.TopicRoot + "/" + topic_name + "/" + part_name + "/" + block_name
	Dups, _, _ := z.Con.Children(BlockPath)
	for _, dup_name := range Dups {
		DupNode, err := z.GetDuplicateNode(BlockPath + "/" + dup_name)
		if err != nil {
			//
		} else {
			nodes = append(nodes, DupNode)
		}
	}
	return nodes
}

func (z *ZK) UpdateBlockNode(bnode BlockNode) error {
	path := z.TopicRoot + "/" + bnode.Topic + "/" + bnode.Partition + "/" + bnode.Name
	ok, _, err := z.Con.Exists(path)
	if !ok {
		return err
	}
	data, err := json.Marshal(bnode)
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

// 检查broker是否在线
func (z *ZK) CheckBroker(brokerpath string) bool {
	brokerpath = brokerpath + "/state"
	ok, _, _ := z.Con.Exists(brokerpath)
	if !ok {
		return false
	} else {
		return true
	}
}

// 从 zk 获取该 block 对应的元数据结构
func (z *ZK) GetBlockNode(path string) (BlockNode, error) {
	var bloNode BlockNode
	data, _, err := z.Con.Get(path)
	if err != nil {
		return bloNode, err
	}
	json.Unmarshal(data, &bloNode)
	return bloNode, nil
}
func (z *ZK) GetDuplicateNode(path string) (DuplicateNode, error) {
	var dupNode DuplicateNode
	ok, _, err := z.Con.Exists(path)
	if !ok {
		return dupNode, err
	}
	data, _, err := z.Con.Get(path)
	json.Unmarshal(data, &dupNode)
	return dupNode, nil
}
func (z *ZK) GetBrokerNode(name string) (BrokerNode, error) {
	//这个为什么path和上面的不一样
	path := z.BrokerRoot + "/" + name
	var broNode BrokerNode
	ok, _, err := z.Con.Exists(path)
	if !ok {
		return broNode, err
	}
	data, _, _ := z.Con.Get(name)
	json.Unmarshal(data, &broNode)
	return broNode, nil
}

type StartGetInfo struct {
	CliName       string
	TopicName     string
	PartitionName string
	Option        int8
}

// 这个还没有实现
func (z *ZK) CheckSub(info StartGetInfo) bool {
	return true
}

// 创建临时节点
func (z *ZK) CreateState(brokerName string) error {
	path := z.BrokerRoot + "/" + brokerName + "/state"
	ok, _, err := z.Con.Exists(path)
	if !ok {
		return err
	}
	_, err = z.Con.Create(path, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}
func (z *ZK) GetPartBlockIndex(topicname, partname string) (int64, error) {
	//str:=z.TopicRoot+"/"+topicname+"/Partitions/"+partname
	node, err := z.GetPartitionNode(topicname, partname)
	if err != nil {
		return 0, err
	}
	return node.Index, nil
}
