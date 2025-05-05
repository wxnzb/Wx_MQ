package server

import (
	"errors"
	"sync"
	"time"
)

const (
	TOPIC_NIL_PTP = 1
	TOPIC_NIL_PSB = 2
	TOPIC_KEY_PSB = 3
)

// 一个Topic下面对应多个分区，通过map以分区名字将分区连接，没个分区需要由读写锁还有一个byte的队列
type Topic struct {
	rmu     sync.RWMutex
	Parts   map[string]*Partition    //key一般是分区的名字，value是分区的指针
	SubList map[string]*SubScription //key是topicname+option类型，value是消费者订阅的信息
}

// 一个分区需要哪些信息？这个分区肯定要有自己的名字，这个分区肯定存了很多信息在队列，这里先用string类型，这个分区里面可定有很多消费者，每个消费者都应该知道他消费到哪条信息了，因此还需要偏移量
type Partition struct {
	rmu             sync.RWMutex
	key             string
	queue           []string
	consumer_offset map[string]int //这里string是某个消费者的唯一标识，int就是他消费到的偏移量
}

// 现在要我想，他就需要在哪个topic以及哪个分区，感觉没了
type SubScription struct {
	name               string //topicname+option类型
	rmu                sync.RWMutex
	topic_name         string
	consumer_partition map[string]string //一个消费者可以属于多个消费者组
	groups             []*Group
	option             int8
	consistent         *Consistent
}
type Consistent struct {
}

func (t *Topic) StartRelease(s *Server) {
	for _, part := range t.Parts {
		part.Release(s)
	}
}
func (p *Partition) Release(s *Server) {
	for consumer_name := range p.consumer_offset {
		s.rmu.Lock()
		con := s.consumers[consumer_name]
		s.rmu.Unlock()
		//开启新的携程服务端主动向消费者推送消息
		go p.Pub(con)
	}
}
func (p *Partition) Pub(con *Consumer) {
	//要是客户端活着，先得到消息
	for {
		con.rmu.RLock()
		//cl.state=="alive写成这样可以吗
		if con.state == ALIVE {
			name := con.name
			con.rmu.RUnlock()
			p.rmu.RLock()
			offset := p.consumer_offset[name]
			msg := p.queue[offset]
			p.rmu.Unlock()
			ret := con.Pub(msg)
			if ret {
				p.rmu.Lock()
				p.consumer_offset[name] = offset + 1
				p.rmu.Unlock()
			}
		} else {
			con.rmu.RUnlock()
			time.Sleep(5 * time.Second)
		}
	}
}

// ///////////////
func (t *Topic) AddScription(req Sub, con *Consumer) (*SubScription, error) {
	ret := t.getStringFromSub(req)
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
	//t.Parts.AddConsumer(con)??这个记得明天写一下，加上con就是为他服务的
	t.Rebalance()
	return subScription, nil
}

// 减少一个订阅，如果订阅存在就删出他，并重新进行负载均衡
func (t *Topic) ReduceScription(req Sub) (string, error) {
	ret := t.getStringFromSub(req)
	t.rmu.Lock()
	_, ok := t.SubList[ret]
	t.rmu.RUnlock()
	if ok {
		delete(t.SubList, ret)
	} else {
		errors.New("订阅不存在")
	}
	t.rmu.Unlock()
	t.Rebalance()
	return ret, nil
}

// topic + "nil" + "ptp" (point to point consumer比partition为 1 : n)
// topic + key   + "psb" (pub and sub consumer比partition为 n : 1)
// topic + "nil" + "psb" (pub and sub consumer比partition为 n : n)
func (t *Topic) getStringFromSub(sub Sub) string {
	ret := sub.topic
	if sub.option == TOPIC_NIL_PTP {
		ret = ret + "nil" + "ptp"
	} else if sub.option == TOPIC_NIL_PSB {
		ret = ret + "nil" + "psb"
	} else if sub.option == TOPIC_KEY_PSB {
		ret = ret + sub.key + "psb"
	}
	return ret
}

// 将消费者加入到这个订阅队列里面
func (sub *SubScription) AddConsumer(req Sub) {
	switch req.option {
	//点对点订阅
	case TOPIC_NIL_PTP:
		{
			//sub.groups[0].consumers[req.consumer]=true
			sub.groups[0].AddConsumer(req.consumer)
		}
	//按key发布的订阅
	case TOPIC_KEY_PSB:
		{
			group := NewGroup(req.topic, req.consumer)
			sub.groups = append(sub.groups, group)
			sub.consumer_partition[req.consumer] = req.key
		}
	}
}

// // 现在要我想，他就需要在哪个topic以及哪个分区，感觉没了
//
//	type SubScription struct {
//		mu                 sync.RWMutex
//		topic_name         string
//		consumer_partition map[string]string //一个消费者可以属于多个消费者组
//		groups             []*Group
//		option             int8
//	}
//
// 这里默认就是TOPIC_KEY_PSB形式
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
	if sub.option == TOPIC_NIL_PTP {
		//初始化哈希，然后将这个消费者加进去
		subScription.consistent = NewConsistent()
		subScription.consistent.Add(sub.consumer)
	}
	return subScription
}
func NewConsistent() *Consistent {

}
func (cons *Consistent) Add(consumer string) {

}

// PushRequest
func (topic *Topic) AddMessage(s *Server, push Push) error {
	part, ok := topic.Parts[push.key]
	if !ok {
		//要是没有这个分区，就要创建一个新的分区
		part := NewPartition(push)
		//这句还需要在理解
		//先在这是一个分区，要是有消费者订阅，通过这个携程可以及时推送给他
		//这里真的需要用携程吗？
		go part.Release(s)
		topic.Parts[push.key] = part

	} else {
		part.rmu.Lock()
		part.queue = append(part.queue, push.message)
		part.rmu.Unlock()
	}
	return nil
}

// 创建一个新的topic
func NewTopic(push Push) *Topic {
	topic := &Topic{
		rmu:     sync.RWMutex{},
		Parts:   make(map[string]*Partition),
		SubList: make(map[string]*SubScription),
	}
	topic.Parts[push.key] = NewPartition(push)
	return topic
}

// 创建一个新的分区
func NewPartition(push Push) *Partition {
	part := &Partition{
		rmu:             sync.RWMutex{},
		key:             push.key,
		queue:           make([]string, 40),
		consumer_offset: make(map[string]int),
	}
	part.queue = append(part.queue, push.message)
	return part
}
func (sub *SubScription) shutDownConsumer(consumer_name string) string {
	sub.rmu.Lock()
	switch sub.option {
	case TOPIC_NIL_PTP:
		{
			sub.groups[0].DownConsumer(consumer_name)
		}
	case TOPIC_KEY_PSB:
		{
			for _, group := range sub.groups {
				group.DownConsumer(consumer_name)
			}
		}
	}
	sub.rmu.Unlock()
	sub.Rebalance()
	return sub.topic_name
}
func (sub *SubScription) Rebalance() {

}
func (t *Topic) Rebalance() {

}
func (t *Topic) RecoverConsumer(sub_name string, con *Consumer) {

}
