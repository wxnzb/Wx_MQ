package server

import (
	"errors"
	"hash/crc32"
	"os"
	"sort"
	"strconv"
	"sync"
	//"time"
)

const (
	OFFSET        = 0
	TOPIC_NIL_PTP = 1
	TOPIC_NIL_PSB = 2
	TOPIC_KEY_PSB = 3
	VIRTUAL_10    = 10
	VIRTUAL_20    = 20
)

type Topic struct {
	rmu     sync.RWMutex
	Parts   map[string]*Partition    //分区列表
	SubList map[string]*SubScription //订阅列表
	Files   map[string]string
}

// 创建一个新的topic
func NewTopic(push Push) *Topic {
	t := &Topic{
		rmu:     sync.RWMutex{},
		Parts:   make(map[string]*Partition),
		SubList: make(map[string]*SubScription),
		Files:   make(map[string]string),
	}
	str, _ := os.Getwd()
	str += "/" + ip_name + "/" + push.topic
	CreateDir(str)
	part, file := NewPartition(push)
	t.Parts[push.key] = part
	t.Files[push.key] = file
	return t
}

// // 开始server.make()的时候调用
// func (t *Topic) StartRelease(s *Server) {
// 	for _, part := range t.Parts {
// 		part.Release(s)
// 	}
// }

// PushRequest
func (t *Topic) AddMessage(push Push) error {
	part, ok := t.Parts[push.key]
	if !ok {
		//要是没有这个分区，就要创建一个新的分区
		part, file := NewPartition(push)
		t.Files[push.key] = file
		t.Parts[push.key] = part
	}
	part.rmu.Lock()
	//part.queue = append(part.queue, push.message)
	part.addMessage(push)
	part.rmu.Unlock()

	return nil
}

// 消费者要订阅topic里面的一个分区
func (t *Topic) AddScription(req Sub, con *ToConsumer) (*SubScription, error) {
	ret := GetStringFromSub(req.topic, req.key, req.option)
	//说明这个订阅已经存在了，将新的消费者加入到订阅这个的列表里面
	t.rmu.RLock()
	subScription, ok := t.SubList[ret]
	t.rmu.RUnlock()
	if ok {
		subScription.AddConsumer(req)
	} else {
		subScription = NewSubScription(req, ret)
		//更新订阅列表
		t.rmu.Lock()
		t.SubList[ret] = subScription
		t.rmu.Unlock()
	}
	//t.Parts.AddConsumer(con)这个现在还不太清楚，先把他删了把
	//t.Rebalance()
	return subScription, nil
}

// 减少一个订阅，如果订阅存在就删出他，并重新进行负载均衡
func (t *Topic) ReduceScription(req Sub) (string, error) {
	ret := GetStringFromSub(req.topic, req.key, req.option)
	t.rmu.Lock()
	subscription, ok := t.SubList[ret]
	if ok {
		//delete(t.SubList, ret)
		//这个是新加的
		subscription.ReduceConsumer(req.consumer)
	} else {
		errors.New("订阅不存在")
	}
	delete(t.SubList, ret)
	t.rmu.Unlock()
	//t.Rebalance()
	return ret, nil
}

// topic + "nil" + "ptp" (point to point consumer比partition为 1 : n)
// topic + key   + "psb" (pub and sub consumer比partition为 n : 1)
// topic + "nil" + "psb" (pub and sub consumer比partition为 n : n)
func GetStringFromSub(topic_name, partition_name string, option int8) string {
	ret := topic_name
	if option == TOPIC_NIL_PTP {
		ret = ret + "nil" + "ptp"
	} else if option == TOPIC_NIL_PSB {
		ret = ret + "nil" + "psb"
	} else if option == TOPIC_KEY_PSB {
		ret = ret + partition_name + "psb"
	}
	return ret
}
func (t *Topic) Rebalance() {

}
func (t *Topic) RecoverConsumer(sub_name string, con *ToConsumer) {

}
func (t *Topic) GetFile(partition string) *File {
	t.rmu.RLock()
	defer t.rmu.RUnlock()
	return t.Parts[partition].GetFile()
}

// ---------------------------------------------------------------------------
type Partition struct {
	rmu   sync.RWMutex
	key   string   //分区名字
	queue []string //分区里面存的消息队列
	// consumer_offset map[string]int         //消费者偏移列表
	// consumer        map[string]*ToConsumer //消费者列表
	//新加的关于文件的,下面这些到底是干什么用的
	file_name   string
	file        *File
	fd          *os.File
	index       int64
	start_index int64
}

// 创建一个新的分区
func NewPartition(req Push) (*Partition, string) {
	part := &Partition{
		rmu:   sync.RWMutex{},
		key:   req.key,
		queue: make([]string, 40),
		//consumer_offset: make(map[string]int),
		//consumer:make(map[string]*ToConsumer),不用加这句吗
	}
	str, _ := os.Getwd()
	str += "/" + ip_name + "/" + req.topic + "/" + req.key + ".txt"
	part.file_name = str
	part.file = NewFile(str)
	file, err := CreateFile(str)
	if err != nil {

	}
	part.fd = file
	part.index = part.file.GetIndex(part.fd)
	part.start_index = part.index + 1
	return part, str
}

// 在新创建了一个分区之后，要做的是，发布消息给所有分区的消费者，但是现在还没有消费者呀，好奇怪？？？？
// func (p *Partition) Release(s *Server) {
// 	for consumer_name := range p.consumer_offset {
// 		s.rmu.Lock()
// 		con := s.consumers[consumer_name]
// 		s.rmu.Unlock()
// 		//开启新的携程服务端主动向消费者推送消息
// 		go p.Pub(con)
// 	}
// }

// 发布消息给特定的消费者，根据消费者的状态决定是否继续发送消息
// func (p *Partition) Pub(con *ToConsumer) {
// 	//要是消费者客户端活着，他会从offset那里之后接收消息，但是要是到最后一个消息了呢？？
// 	for {
// 		con.rmu.RLock()
// 		//cl.state=="alive写成这样可以吗，当然可以
// 		if con.state == ALIVE {
// 			name := con.name
// 			con.rmu.RUnlock()
// 			p.rmu.RLock()
// 			offset := p.consumer_offset[name]
// 			msg := p.queue[offset]
// 			p.rmu.RUnlock()
// 			//这一句很关键，应该是服务器将从队列取出的消息传递给消费者！！！！
// 			ret := con.Pub(msg)
// 			if ret {
// 				p.rmu.Lock()
// 				p.consumer_offset[name] = offset + 1
// 				p.rmu.Unlock()
// 			}
// 		} else {
// 			con.rmu.RUnlock()
// 			time.Sleep(5 * time.Second)
// 		}
// 	}
// }

// 将消费者添加到这个分区
//
//	func (p *Partition) AddConsumer(con *ToConsumer) {
//		p.rmu.Lock()
//		defer p.rmu.Unlock()
//		p.consumer[con.name] = con
//		p.consumer_offset[con.name] = OFFSET
//	}
//
//	func (p *Partition) DeleteConsumer(con *ToConsumer) {
//		p.rmu.Lock()
//		defer p.rmu.Unlock()
//		delete(p.consumer, con.name)
//		delete(p.consumer_offset, con.name)
//	}
func (p *Partition) GetFile() *File {
	p.rmu.RLock()
	defer p.rmu.Unlock()
	return p.file
}
func (p *Partition) addMessage(req Push) {

}

// -----------------------------------------------------------
type SubScription struct {
	name               string //topicname+option类型
	rmu                sync.RWMutex
	topic_name         string
	consumer_partition map[string]string //一个消费者对应的分区
	groups             []*Group
	option             int8
	//consistent         *Consistent
}

// 创建一个新的SubScription,这里默认就是TOPIC_KEY_PSB形式
func NewSubScription(sub Sub, ret string) *SubScription {
	subScription := &SubScription{
		rmu:                sync.RWMutex{},
		topic_name:         sub.topic,
		consumer_partition: make(map[string]string),
		option:             sub.option,
		name:               ret,
	}
	group := NewGroup(sub.topic, sub.consumer)
	subScription.groups = append(subScription.groups, group)
	subScription.consumer_partition[sub.consumer] = sub.key
	//直接在这里加上点对点还是先有点问题
	// if sub.option == TOPIC_NIL_PTP {
	// 	//初始化哈希，然后将这个消费者加进去
	// 	subScription.consistent = NewConsistent()
	// 	subScription.consistent.Add(sub.consumer)
	// }
	return subScription
}

// ---这个也不要了吗
// 将消费者加入到这个订阅队列里面
func (sub *SubScription) AddConsumer(req Sub) {
	switch req.option {
	//点对点订阅，全部放在一个消费者组里面
	case TOPIC_NIL_PTP:
		{
			//sub.groups[0].consumers[req.consumer]=true
			sub.groups[0].AddConsumer(req.consumer)
		}
	//按key发布的订阅，创建新的消费者组
	case TOPIC_KEY_PSB:
		{
			group := NewGroup(req.topic, req.consumer)
			sub.groups = append(sub.groups, group)
			sub.consumer_partition[req.consumer] = req.key
		}
	}
}

// 将消费者从订阅队列里面移除，这里没有真正删除，只是将他标为不活跃
func (sub *SubScription) shutDownConsumer(consumer_name string) string {
	sub.rmu.Lock()
	switch sub.option {
	//点对点就只有一个消费者组group[0],因此无需遍历直接删除就好，用下面那个我感觉也是没有问题的
	case TOPIC_NIL_PTP:
		{
			sub.groups[0].DownConsumer(consumer_name)
			//sub.consistent.Reduce(consumer_name) //为啥这个需要下面哪个不需要？？？？？

		}
		//因为广播的话有很多消费者组，你需要在这里面先找到消费者，然后将他标记为不活跃
	case TOPIC_KEY_PSB:
		{
			for _, group := range sub.groups {
				group.DownConsumer(consumer_name)
			}
		}
	}
	sub.rmu.Unlock()
	//sub.Rebalance()好像不需要这句
	return sub.topic_name
}
func (sub *SubScription) ReduceConsumer(consumer_name string) {
	sub.rmu.Lock()
	switch sub.option {
	case TOPIC_NIL_PTP:
		{
			sub.groups[0].DeleteConsumer(consumer_name)
			//sub.consistent.Reduce(consumer_name)

		}
	case TOPIC_KEY_PSB:
		{
			for _, group := range sub.groups {
				group.DeleteConsumer(consumer_name)
			}
		}
	}
	sub.rmu.Unlock()
}

// 恢复消费者
func (sub *SubScription) RecoverConsumer(req Sub) {
	sub.rmu.Lock()
	switch sub.option {
	case TOPIC_NIL_PTP:
		{
			sub.groups[0].RecoverConsumer(req.consumer)
			//sub.consistent.Add(req.consumer)
		}
	case TOPIC_KEY_PSB:
		{
			group := NewGroup(req.topic, req.consumer)
			sub.groups = append(sub.groups, group)
			sub.consumer_partition[req.consumer] = req.key
		}
	}
	sub.rmu.Unlock()
}
func (sub *SubScription) Rebalance() {

}

// -------------------------------------------------------------
type Consistent struct {
	rmu              sync.RWMutex
	hashSortNodes    []uint32          //排序的虚拟节点
	circleNodes      map[uint32]string //虚拟节点对应的世纪节点
	virtualNodeCount int               //虚拟节点数量
	nodes            map[string]bool   //已绑定的世纪节点为true，这个还不太了解
}

func NewConsistent() *Consistent {
	return &Consistent{
		rmu:              sync.RWMutex{},
		hashSortNodes:    make([]uint32, 0),
		circleNodes:      make(map[uint32]string),
		virtualNodeCount: VIRTUAL_10,
		nodes:            make(map[string]bool),
	}
}
func (c *Consistent) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
func (c *Consistent) Add(node string) error {
	if node == "" {
		return nil
	}
	c.rmu.Lock()
	defer c.rmu.Unlock()
	ok := c.nodes[node]
	if ok {
		return errors.New("node already exist")
	}
	c.nodes[node] = true
	for i := 0; i < c.virtualNodeCount; i++ {
		virtualnode := c.hashKey(node + strconv.Itoa(i))
		c.circleNodes[virtualnode] = node
		c.hashSortNodes = append(c.hashSortNodes, virtualnode)
	}
	sort.Slice(c.hashSortNodes, func(i, j int) bool {
		return c.hashSortNodes[i] < c.hashSortNodes[j]
	})
	return nil
}
func (c *Consistent) Reduce(node string) error {
	if node == "" {
		return nil
	}
	c.rmu.Lock()
	defer c.rmu.Unlock()
	ok := c.nodes[node]
	if !ok {
		return errors.New("node not exist")
	}
	c.nodes[node] = false
	for i := 0; i < c.virtualNodeCount; i++ {
		virtualnode := c.hashKey(node + strconv.Itoa(i))
		delete(c.circleNodes, virtualnode)
		for j := 0; j < len(c.hashSortNodes); j++ {
			if c.hashSortNodes[j] == virtualnode && j != len(c.hashSortNodes)-1 {
				c.hashSortNodes = append(c.hashSortNodes[:j], c.hashSortNodes[j+1:]...)
			} else if c.hashSortNodes[j] == virtualnode && j == len(c.hashSortNodes)-1 {
				c.hashSortNodes = c.hashSortNodes[:j]
			}
		}
	}
	sort.Slice(c.hashSortNodes, func(i, j int) bool {
		return c.hashSortNodes[i] < c.hashSortNodes[j]
	})
	return nil
}

// 一致性哈希环的顺时针查找最近节点
func (c *Consistent) GetNode(key string) string {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	hashKey := c.hashKey(key)
	i := c.getposition(hashKey)
	return c.circleNodes[c.hashSortNodes[i]]
}
func (c *Consistent) getposition(hashKey uint32) int {
	i := sort.Search(len(c.hashSortNodes), func(i int) bool {
		return c.hashSortNodes[i] >= hashKey
	})
	if i == len(c.hashSortNodes) {
		return 0
	}
	return i
}
