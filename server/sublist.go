package server

import (
	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/client_operations"
	"Wx_MQ/kitex_gen/api/zkserver_operations"
	"Wx_MQ/logger"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"strconv"
	"sync"
	//"time"
)

const (
	OFFSET = 0

	TOPIC_NIL_PTP_PUSH = 1
	TOPIC_NIL_PTP_PULL = 2
	TOPIC_KEY_PSB_PUSH = 3
	TOPIC_KEY_PSB_PULL = 4
	//下面这个主要是干什么的？
	TOPIC_NIL_PSB = 10
	VIRTUAL_10    = 10
	VIRTUAL_20    = 20
)

type Topic struct {
	rmu     sync.RWMutex
	Parts   map[string]*Partition    //分区列表
	SubList map[string]*SubScription //订阅列表
	Files   map[string]*File
	Name    string
	Broker  string
}

// 创建一个新的topic,这里push里面必须要有topic和key
func NewTopic(brokerName, topicName string) *Topic {
	t := &Topic{
		rmu:     sync.RWMutex{},
		Parts:   make(map[string]*Partition),
		SubList: make(map[string]*SubScription),
		Files:   make(map[string]*File),
		Name:    topicName,
		Broker:  brokerName,
	}
	str, _ := os.Getwd()
	//str += "/" + ip_name + "/" + topicName
	str += "/" + brokerName + "/" + topicName
	err := CreateDir(str)
	if err != nil {
		logger.DEBUG(logger.DError, "create list failed %v\n", err.Error())
		return nil
	}
	return t
}
func (t *Topic) AddPartition(partName string) {
	part := NewPartition(t.Name, partName)
	t.rmu.Lock()
	t.Parts[partName] = part
	t.rmu.Unlock()
}

// PushRequest，这里添加将消息写入分区
func (t *Topic) AddMessage(req Info) error {
	t.rmu.RLock()
	part, ok := t.Parts[req.partition]
	t.rmu.RUnlock()
	if !ok {
		logger.DEBUG(logger.DError, "not find this part in add message\n")
		//要是没有这个分区，就要创建一个新的分区
		part = NewPartition(t.Broker, req.topic, req.partition)
		t.rmu.Lock()
		t.Parts[req.partition] = part
		t.rmu.Unlock()
	}
	logger.DEBUG(logger.DLog, "topic(%v) use partition(%v) addMessage\n", t.Name, req.partition)
	part.AddMessage(req)

	return nil
}

// 消费者要订阅topic里面的一个分区
func (t *Topic) AddScription(in Info) (*SubScription, error) {
	ret := GetStringFromSub(in.topic, in.partition, in.option)
	//说明这个订阅已经存在了，将新的消费者加入到订阅这个的列表里面
	t.rmu.RLock()
	subScription, ok := t.SubList[ret]
	t.rmu.RUnlock()
	if ok {
		subScription.AddConsumer(in)
	} else {
		//后面那两个主要是干啥的，为啥要传进去
		subScription = NewSubScription(in, ret, t.Parts, t.Files)
		//更新订阅列表
		t.rmu.Lock()
		t.SubList[ret] = subScription
		t.rmu.Unlock()
	}
	return subScription, nil
}

// 减少一个订阅，如果订阅存在就删出他，并重新进行负载均衡
func (t *Topic) ReduceScription(in Info) (string, error) {
	ret := GetStringFromSub(in.topic, in.partition, in.option)
	t.rmu.Lock()
	subscription, ok := t.SubList[ret]
	if ok {
		subscription.ReduceConsumer(in)
	} else {
		return ret, errors.New("订阅不存在")
	}
	delete(t.SubList, ret)
	t.rmu.Unlock()
	return ret, nil
}

// 根据订阅选项生成订阅字符串
// topic + "nil" + "ptp" (point to point consumer比partition为 1 : n)
// topic + key   + "psb" (pub and sub consumer比partition为 n : 1)按键分发
func GetStringFromSub(topic_name, partition_name string, option int8) string {
	ret := topic_name
	if option == TOPIC_NIL_PTP_PUSH || option == TOPIC_NIL_PTP_PULL {
		ret = ret + "NIL" + "ptp"
	} else if option == TOPIC_KEY_PSB_PUSH || option == TOPIC_KEY_PSB_PULL {
		ret = ret + partition_name + "psb"
	}
	return ret
}
func (t *Topic) Rebalance() {

}
func (t *Topic) RecoverConsumer(sub_name string, con *ToConsumer) {

}
func (t *Topic) GetFile(in Info) (File *File, Fd *os.File) {
	t.rmu.Lock()
	str, _ := os.Getwd()
	str += "/" + t.Broker + "/" + in.topic + "/" + in.partition + "/" + in.file_name
	file, ok := t.Files[str]
	if !ok {
		file, fd, _, err := NewFile(str)
		if err != nil {
			return nil, nil
		}
		Fd = fd
		t.Files[str] = file
	} else {
		Fd = file.OpenFileRead()
	}
	t.rmu.Unlock()
	return File, Fd
}
func (t *Topic) GetParts() map[string]*Partition {
	t.rmu.RLock()
	defer t.rmu.RUnlock()
	return t.Parts
}

func (t *Topic) prepareAcceptHandle(in Info) (ret string, err error) {
	t.rmu.Lock()

	part, ok := t.Parts[in.partition]
	if !ok {
		part = NewPartition(in.topic, in.partition)
		t.Parts[in.partition] = part
	}
	//设置partition值噢部分file的fd,start_index等信息
	str, _ := os.Getwd()
	str += "/" + t.Broker + "/" + in.topic + "/" + in.partition + "/" + in.file_name

	file, fd, Err, err := NewFile(str)
	if err != nil {
		return Err, err
	}
	t.rmu.Unlock()
	ret = part.StartGetMessage(file, fd, in)
	if ret == OK {

	} else {

	}
	return ret, nil
}
func (t *Topic) closeAcceptHandle(in Info) (start, end int64, ret string, err error) {
	t.rmu.RLock()
	part, ok := t.Parts[in.partition]
	t.rmu.RUnlock()
	if !ok {
		ret = "this partition is not in this broker"
		return 0, 0, ret, errors.New(ret)
	}
	start, end, ret, err = part.CloseAcceptMessage(in)
	if err != nil {

	} else {
		str, _ := os.Getwd()
		str += "/" + t.Broker + "/" + in.topic + "/" + in.partition + "/"
		t.rmu.Lock()
		t.Files[str+in.newfile_name] = t.Files[str+in.file_name]
		delete(t.Files, str+in.file_name)
		t.rmu.Unlock()
	}
	return start, end, ret, err
}
func (t *Topic) prepareSendHandle(in Info, zkclient *zkserver_operations.Client) (ret string, err error) {
	sub_name := GetStringFromSub(in.topic, in.partition, in.option)
	t.rmu.Lock()
	//检查或创建partition
	part, ok := t.Parts[in.partition]
	if !ok {
		part = NewPartition(t.Broker, t.Name, in.partition)
		t.Parts[in.partition] = part
	}
	//检查文件是否存在
	str, _ := os.Getwd()
	str += "/" + t.Broker + "/" + in.topic + "/" + in.partition + "/" + in.file_name
	file, ok := t.Files[str]
	if !ok {
		file_ptr, fd, Err, err := NewFile(str)
		if err != nil {
			return Err, err
		}
		fd.Close()
		file = file_ptr
		t.Files[str] = file
	}
	//检查或创建sub
	sub, ok := t.SubList[sub_name]
	if !ok {
		sub = NewSubScription(in, sub_name, t.Parts, t.Files)
		t.SubList[sub_name] = sub
	}
	t.rmu.Unlock()
	//在sub中创建对应文件的config,等待startget
	if in.option == TOPIC_NIL_PTP_PUSH {
		ret, err = sub.AddPTPConfig(in, part, file, zkclient)
	} else if in.option == TOPIC_KEY_PSB_PUSH {
		sub.AddPSBConfig(in, in.partition, file, zkclient)
	} else if in.option == TOPIC_NIL_PTP_PULL || in.option == TOPIC_KEY_PSB_PUSH {
		sub.AddNode(in, file)
	}
	return ret, err
}
func (t *Topic) PullMessage(pullRequest Info) (MSGS, error) {
	logger.DEBUG(logger.DLog, "the info %v\n", pullRequest)
	sub_name := GetStringFromSub(pullRequest.topic, pullRequest.partition, pullRequest.option)
	t.rmu.RLock()
	sub, ok := t.SubList[sub_name]
	t.rmu.RUnlock()
	if !ok {
		str := fmt.Sprintf("%v this topic(%v) don't have sub(%v) the sublist is %v for %v\n", t.Broker, t.Name, sub_name, t.subList, in.consumer)
		logger.DEBUG(logger.DError, "%v\n", str)
		return MSGS{}, errors.New("no this sub")
	}
	return sub.PullMessage(pullRequest)

}

// 根据 sub_name 找到对应的订阅（Sub）对象，然后将当前消费者 cli 注册到这个订阅下，并更新其配置（负载均衡相关配置）
func (t *Topic) StartToGetHandle(sub_name string, in Info, consumer_to_broker *client_operations.Client) error {
	t.rmu.RLock()
	defer t.rmu.RUnlock()
	sub, ok := t.SubList[sub_name]
	if !ok {
		ret := "this topic not have this subscription"
		logger.DEBUG(logger.DLog, "%v\n", ret)
		return errors.New(ret)
	}
	sub.AddConsumerInConfig(in, consumer_to_broker)
	return nil
}

// ---------------------------------------------------------------------------
type Partition struct {
	rmu   sync.RWMutex
	key   string    //分区名字
	queue []Message //分区里面存的消息队列
	//新加的关于文件的,下面这些到底是干什么用的
	file_name   string
	file        *File
	fd          *os.File
	index       int64 //文件指向的位置
	start_index int64
	state       string
	Broker      string
}

// 创建一个新的分区
func NewPartition(broker, topic, partition string) *Partition {
	part := &Partition{
		rmu:    sync.RWMutex{},
		key:    partition,
		state:  CLOSE,
		index:  0,
		Broker: broker,
	}
	str, _ := os.Getwd()
	str += "/" + broker + "/" + topic + "/" + partition

	return part
}
func (p *Partition) StartGetMessage(file *File, fd *os.File, in Info) string {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	ret := ""
	switch p.state {
	case ALIVE:
		ret = ErrHadStart
	case CLOSE:
		p.state = ALIVE
		p.file = file
		p.fd = fd
		p.index = file.GetIndex(fd)
		p.start_index = p.index
		ret = OK
	}
	return ret
}
func (p *Partition) CloseAcceptMessage(in Info) (start, end int64, ret string, err error) {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	if p.state == ALIVE {
		str, _ := os.Getwd()
		str += "/" + p.Broker + "/" + in.topic + "/" + in.partition
		p.file.UpFileName(str, in.newfile_name)
		p.file_name = in.newfile_name
		p.state = CLOSE
		end = p.index
		start = p.file.GetFirstIndex(p.fd)
		p.fd.Close()
	} else if p.state == DOWN {
		ret = "this part had close"
		err = errors.New(ret)
	}
	return start, end, ret, err
}

// 内存队列中累积消息，并在队列达到一定长度后批量写入文件
func (p *Partition) AddMessage(req Info) (ret string, err error) {
	p.rmu.Lock()
	if p.state == DOWN {
		ret = "this part has closed"
		logger.DEBUG(logger.DLog, "%v\n", ret)
		return ret, errors.New(ret)
	}
	p.index++
	msg := Message{
		Topic_name:     req.topic,
		Partition_name: req.partition,
		Index:          p.index,
		Msg:            req.message,
		Size:           req.size,
	}
	p.queue = append(p.queue, msg)
	logger.DEBUG(logger.DLog, "part_name(%v) add message %v index is %v size is %v\n", p.key, msg, p.index, p.index-p.start_index)
	//达到一定大小后写入磁盘
	if p.index-p.start_index >= VIRTUAL_10 {
		p.flushToDisk()
	}
	p.rmu.Unlock()
	//你要知道zkerver管一切，因此这个partition变化必须上传给zkserver
	if req.zkclient != nil {
		(*req.zkclient).UpdateDup(context.Background(), &api.UpdateDupRequest{
			Topic: req.topic,
			Part:  req.partition,
			//但是这个是啥时候传进去的？？，还有下面的file_name,真是奇怪？？？
			BrokerName: req.BrokerName,
			BlockName:  GetBlockName(req.file_name),
			EndIndex:   p.index,
		})
	}
	return ret, err
}
func (p *Partition) flushToDisk() {
	if len(p.queue) == 0 {
		return
	}
	node := NodeData{
		Start_index: p.start_index,
		End_index:   p.index,
		Size:        len(p.queue), //这个肯定是 VIRTUAL_10
	}
	msgs_data, _ := json.Marshal(p.queue)
	for !p.file.WriteFile(p.fd, node, msgs_data) {
		logger.DEBUG(logger.DError, "write to %v faile\n", p.file_name)
	}
	p.start_index = p.index + 1
	p.queue = p.queue[:0]
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

// 为partition添加一个文件存储他
func (p *Partition) AddFile(path string) *File {
	return &File{}
}

// -----------------------------------------------------------
type SubScription struct {
	rmu sync.RWMutex

	name               string //topicname+partitionname+option类型
	topic_name         string
	consumer_partition map[string]string //一个消费者对应的分区
	groups             []*Group
	option             int8
	config             *Config
	nodes              map[string]*Node
	partitions         map[string]*Partition
	Files              map[string]*File
	PTP_config         *Config
	PSB_config         map[string]*PSBConfig_PUSH
}

// 创建一个新的SubScription,这里默认就是TOPIC_KEY_PSB形式
func NewSubScription(in Info, name string, parts map[string]*Partition, files map[string]*File) *SubScription {
	subScription := &SubScription{
		rmu:        sync.RWMutex{},
		name:       name,
		topic_name: in.topic,
		option:     in.option,
		partitions: parts,
		Files:      files,
		PTP_config: nil,
		PSB_config: make(map[string]*PSBConfig_PUSH),
		nodes:      make(map[string]*Node),
	}
	group := NewGroup(in.topic, in.consumer)
	subScription.groups = append(subScription.groups, group)
	return subScription
}

// 当消费者需要开始消费时，ptp
// 要是sub中该文件的config存在，就加入该config
// 要是不存在，就创建一个并加入
func (s *SubScription) AddPTPConfig(in Info, part *Partition, file *File, zkclient *zkserver_operations.Client) (ret string, err error) {
	s.rmu.Lock()
	if s.PTP_config == nil {
		s.PTP_config = NewConfig(in.topic, 0, nil, nil)
	}
	err = s.PTP_config.AddPartition(in, part, file, zkclient)
	s.rmu.Unlock()
	if err != nil {
		return ret, err
	}
	return ret, nil
}
func (s *SubScription) AddPSBConfig(in Info, part_name string, file *File, zkclient *zkserver_operations.Client) {
	s.rmu.Lock()
	_, ok := s.PSB_config[part_name+in.consumer]
	if !ok {
		config := NewPSBConfigPush(in, file, zkclient)
		s.PSB_config[part_name+in.consumer] = config
	} else {

	}
	s.rmu.Unlock()
}
func (s *SubScription) AddNode(in Info, file *File) {
	str := in.topic + in.partition + in.consumer
	s.rmu.Lock()
	_, ok := s.nodes[str]
	if !ok {
		node := NewNode(in, file)
		s.nodes[str] = node
	}
	s.rmu.Unlock()
}

// ---这个也不要了吗
// 将消费者加入到这个订阅队列里面
func (sub *SubScription) AddConsumer(in Info) {
	sub.rmu.Lock()
	defer sub.rmu.Unlock()
	switch in.option {
	//点对点订阅，全部放在一个消费者组里面
	case TOPIC_NIL_PTP_PUSH:
		{
			sub.groups[0].AddConsumer(in.consumer)
		}
	//按key发布的订阅，创建新的消费者组
	case TOPIC_KEY_PSB_PUSH:
		{
			group := NewGroup(in.topic, in.consumer)
			sub.groups = append(sub.groups, group)
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
func (sub *SubScription) ReduceConsumer(in Info) {
	sub.rmu.Lock()
	switch in.option {
	case TOPIC_NIL_PTP:
		{
			sub.groups[0].DeleteConsumer(in.consumer)
			sub.PTP_config.DeleteConsumer(in.partition, in.consumer)

		}
	case TOPIC_KEY_PSB:
		{
			for _, group := range sub.groups {
				group.DeleteConsumer(in.consumer)
			}
		}
	}
	sub.rmu.Unlock()
}

// 恢复消费者
func (sub *SubScription) RecoverConsumer(in Info) {
	sub.rmu.Lock()
	switch sub.option {
	case TOPIC_NIL_PTP:
		{
			sub.groups[0].RecoverConsumer(in.consumer)
			//sub.consistent.Add(req.consumer)
		}
	case TOPIC_KEY_PSB:
		{
			group := NewGroup(in.topic, in.consumer)
			sub.groups = append(sub.groups, group)
			//sub.consumer_partition[req.consumer] = req.key
		}
	}
	sub.rmu.Unlock()
}
func (sub *SubScription) Rebalance() {

}

// 从SubScription中获得*Config
func (sub *SubScription) GetConfig() *Config {
	sub.rmu.RLock()
	defer sub.rmu.RUnlock()
	return sub.config
}
func (sub *SubScription) AddConsumerInConfig(req Info, tocon *client_operations.Client) {
	sub.rmu.Lock()
	defer sub.rmu.Unlock()
	//这才是关键,上面的判断早了
	switch req.option {
	case TOPIC_NIL_PTP_PUSH:
		{
			sub.PTP_config.AddToconsumer(req.partition, req.consumer_ipname, tocon)
		}
	case TOPIC_KEY_PSB_PUSH:
		config, ok := sub.PSB_config[req.partition+req.consumer_ipname]
		if !ok {
			logger.DEBUG(logger.DError, "this PSBConfig PUSH id not been\n")
		}
		config.Start(req, tocon)
	}
}

func (pc *PSBConfig_PUSH) Start(req Info, tocon *client_operations.Client) {
	pc.rmu.Lock()
	pc.Cli = tocon
	var names []string
	clis := make(map[string]*client_operations.Client)
	names = append(names, req.consumer_ipname)
	clis[req.consumer_ipname] = tocon
	pc.part.UpdateCons(names, clis)
	pc.part.Start(pc.part_close)
	pc.rmu.Unlock()
}
func (sub *SubScription) PullMessage(pullRequest Info) (MSGS, error) {
	node_name := pullRequest.topic + pullRequest.partition + pullRequest.consumer
	sub.rmu.RLock()
	node, ok := sub.nodes[node_name]
	sub.rmu.RUnlock()
	if !ok {
		logger.DEBUG(logger.DError, "this sub has not have this node(%v)\n", node_name)
		return MSGS{}, errors.New("this sub no have node")
	}
	return node.ReadMSGS(pullRequest)
}

// -------------------------------------------------------------
type Consistent struct {
	rmu              sync.RWMutex
	hashSortNodes    []uint32          //排序的虚拟节点
	circleNodes      map[uint32]string //虚拟节点对应的世纪节点
	virtualNodeCount int               //虚拟节点数量
	nodes            map[string]bool   //已绑定的世纪节点为true，这个还不太了解
	//consumer以负责一个partition则为true
	ConH     map[string]bool
	FreeNode int //一般是len(ConH)
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
func (c *Consistent) Add(node string, power int) error {
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
	//需要了解
	c.ConH[node] = false
	for i := 0; i < c.virtualNodeCount*power; i++ {
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
	con := c.circleNodes[c.hashSortNodes[i]]
	c.ConH[con] = true
	c.FreeNode--
	return con
}
func (c *Consistent) getposition(hashKey uint32) int {
	i := sort.Search(len(c.hashSortNodes), func(i int) bool {
		return c.hashSortNodes[i] >= hashKey
	})
	if i == len(c.hashSortNodes) {
		if i == len(c.hashSortNodes)-1 {
			return 0
		} else {
			return 1
		}
	} else {
		return len(c.hashSortNodes) - 1
	}
}
func (c *Consistent) SetFreeNode() {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	c.FreeNode = len(c.ConH)
}
func (c *Consistent) GetFreeNode() int {
	c.rmu.RLock()
	defer c.rmu.RUnlock()
	return c.FreeNode
}

// 将conH全部设置成false
func (c *Consistent) TurnZero() {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	for k := range c.ConH {
		c.ConH[k] = false
	}
}

// 给这个partition再分配一个除了主节点之外的空闲节点
func (c *Consistent) GetNodeFree(key string) string {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	hash := c.hashKey(key)   //根据key生成一个hash值
	i := c.getposition(hash) //根据hash值找到对应的节点
	i++
	for {
		if i == len(c.hashSortNodes)-1 {
			i = 0
		}
		con := c.circleNodes[c.hashSortNodes[i]] /// 取出节点名称？
		if !c.ConH[con] {
			c.ConH[con] = true
			c.FreeNode--
			return con
		}
		i++
	}
}

// -------------------------------------------------------------
// 提供一个线程安全的管理机制，管理消息传递系统中的分区和消费者的关系
type Config struct {
	rmu sync.RWMutex

	con_nums  int
	part_nums int

	PartToCon map[string][]string //part对应的消费者

	toconsumers map[string]*client_operations.Client //消费者对应的消费者客户端接口
	partitions  map[string]*Partition
	//文件
	files map[string]*File

	parts map[string]*Part //PTP的part

	consistent *Consistent

	part_close chan *Part
}

func NewConfig(topic_name string, part_nums int, partitions map[string]*Partition, files map[string]*File) *Config {
	con := &Config{
		rmu:         sync.RWMutex{},
		con_nums:    0,
		part_nums:   part_nums,
		PartToCon:   make(map[string][]string),
		toconsumers: make(map[string]*client_operations.Client),
		partitions:  partitions,
		parts:       make(map[string]*Part),
		files:       files,
		consistent:  NewConsistent(),
		part_close:  make(chan *Part),
	}
	go con.GetCloseChan(con.part_close)
	return con
}
func (c *Config) GetCloseChan(ch chan *Part) {
	for close := range c.part_close {
		c.DeletePart(close.partition_name, close.file)
	}
}

// part消费完成，一处config中的Partition和Part
func (c *Config) DeletePart(part string, file *File) {
	c.rmu.Lock()
	c.part_nums--
	delete(c.partitions, part)
	delete(c.files, file.filePath)
	c.rmu.Unlock()
	c.RebalancePtoC() //更新配置
	c.UpdateParts()   //应用配置
}
func (c *Config) AddPartition(in Info, part *Partition, file *File, zkclient *zkserver_operations.Client) error {
	c.rmu.Lock()
	c.part_nums++
	c.partitions[in.partition] = part
	c.files[file.filePath] = file
	c.parts[in.partition] = NewPart(in, file, zkclient)
	c.parts[in.partition].Start(c.part_close)
	c.rmu.Unlock()
	c.RebalancePtoC() //更新配置
	c.UpdateParts()   //应用配置
	return nil
}

// 这里+-知识操作了config,最后要移到part里面去
func (c *Config) AddToconsumer(part string, consumer string, toconsumer *client_operations.Client) {
	c.rmu.Lock()
	c.toconsumers[consumer] = toconsumer
	c.con_nums++
	err := c.consistent.Add(consumer, 1)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	c.rmu.Unlock()
	c.RebalancePtoC() //更新配置
	c.UpdateParts()   //应用配置
}

// 从配置中删除一个 client（consumer）在指定 partition 上的绑定关系，并进行负载均衡
func (c *Config) DeleteConsumer(part, consumer string) {

}
func GetPartitionArry(partitions map[string]*Partition) []string {
	var arry []string
	for name, _ := range partitions {
		arry = append(arry, name)
	}
	return arry
}
func GetConsArry(toconsumers map[string]*client_operations.Client) []string {
	var arry []string
	for name, _ := range toconsumers {
		arry = append(arry, name)
	}
	return arry
}
func TurnConsistent(arry []string) *Consistent {
	consistent := NewConsistent()
	//这里还挺奇怪的，不就是string数组吗，为啥这里要多加一个_???
	for _, name := range arry {
		consistent.Add(name)
	}
	return consistent
}

// 负载均衡，将调整后的配置存入PartToCon
// 将Consisitent中的ConH置false, 循环两次Partitions
// 第一次拿取 1个 Consumer
// 第二次拿取 靠前的 ConH 为 true 的 Consumer
// 直到遇到ConH为 false 的
func (c *Config) RebalancePtoC() {
	//清空之前记录的负载状态，准备好新的分配（rebalance）
	c.consistent.SetFreeNode() //将空闲节点设为len(consumers)
	c.consistent.TurnZero()    //将consumer全设置成空闲

	var parttocon = make(map[string][]string)
	c.rmu.RLock()
	Parts := c.partitions
	c.rmu.RUnlock()
	//为每个 Partition 分配一个主节点
	for name := range Parts {
		node := c.consistent.GetNode(name)
		var arry []string
		arry, ok := parttocon[name]
		//这是因为一个partition对应多个消费者
		arry = append(arry, node)
		if !ok {
			parttocon[name] = arry
		}
	}
	//将空闲的consumer节点（free node）尽可能均匀地分配给每一个partition，构建副本（副消费者）列表
	for {
		for name := range Parts {
			if c.consistent.GetFreeNode() > 0 {
				node := c.consistent.GetNodeFree(name)
				var arry []string
				arry, ok := parttocon[name]
				arry = append(arry, node)
				if !ok {
					parttocon[name] = arry
				}
			} else {
				break
			}
		}
		if c.consistent.GetFreeNode() <= 0 {
			break
		}
	}
	c.rmu.Lock()
	c.PartToCon = parttocon
	c.rmu.Unlock()
	return
}
func (c *Config) UpdateParts() {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	for part_name, part := range c.parts {
		part.UpdateCons(c.PartToCon[part_name], c.toconsumers)
	}
}
func (c *Config) DeleteToconsumer(part, consumer string) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	delete(c.toconsumers, consumer)
	for i, name := range c.PartToCon[part] {
		if name == consumer {
			//后面那三个点是什么
			c.PartToCon[part] = append(c.PartToCon[part][:i], c.PartToCon[part][i+1:]...)
			break
		}
	}
	c.con_nums--
	if c.con_nums < c.part_nums && !c.con_part {
		c.con_part = true
		c.consistent = TurnConsistent(GetConsArry(c.toconsumers))
	}
	if c.con_part == true {
		err := c.consistent.Reduce(consumer)
		if err != nil {
			DEBUG(dERROR, err.Error())
		}
	}
	c.RebalancePtoC() //更新配置
	c.UpdateParts()   //应用配置
}

type PSBConfig_PUSH struct {
	rmu        sync.RWMutex
	part_close chan *Part
	file       *File
	Cli        *client_operations.Client
	part       *Part
}

func NewPSBConfigPush(in Info, file *File, zkclient *zkserver_operations.Client) *PSBConfig_PUSH {
	ret := &PSBConfig_PUSH{
		rmu:        sync.RWMutex{},
		part_close: make(chan *Part),
		file:       file,
		part:       NewPart(in, file, zkclient),
	}
	return ret
}
