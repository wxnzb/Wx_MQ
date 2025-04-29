package server

import (
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
	rmu                sync.RWMutex
	topic_name         string
	consumer_partition map[string]string //一个消费者可以属于多个消费者组
	groups             []*Group
	option             int8
}

func (t *Topic) StartRelease(s *Server) {
	for _, part := range t.Parts {
		part.Release(s)
	}
}
func (p *Partition) Release(s *Server) {
	for consumer_name := range p.consumer_offset {
		s.rmu.Lock()
		cl := s.consumers[consumer_name]
		s.rmu.Unlock()
		go p.Pub(cl)
	}
}
func (p *Partition) Pub(cl *Client) {
	//要是客户端活着，先得到消息
	for {
		cl.rmu.RLock()
		//cl.state=="alive写成这样可以吗
		if cl.state == ALIVE {
			name := cl.name
			cl.rmu.RUnlock()
			p.rmu.RLock()
			offset := p.consumer_offset[name]
			msg := p.queue[offset]
			p.rmu.Unlock()
			ret := cl.Pub(msg)
			if ret {
				p.rmu.Lock()
				p.consumer_offset[name] = offset + 1
				p.rmu.Unlock()
			}
		} else {
			cl.rmu.Unlock()
			time.Sleep(5 * time.Second)
		}
	}
}

// ///////////////
func (t *Topic) AddScription(sub Sub) (*SubScription, error) {
	ret := t.getStringFromSub(sub)
	subScription, ok := t.SubList[ret]
	if ok {

	} else {
		subScription = NewSubScription(sub)
	}
	return subScription, nil
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

// // 现在要我想，他就需要在哪个topic以及哪个分区，感觉没了
//
//	type SubScription struct {
//		mu                 sync.RWMutex
//		topic_name         string
//		consumer_partition map[string]string //一个消费者可以属于多个消费者组
//		groups             []*Group
//		option             int8
//	}
func NewSubScription(sub Sub) *SubScription {
	subScription := &SubScription{
		rmu:                sync.RWMutex{},
		topic_name:         sub.topic,
		consumer_partition: make(map[string]string),
		option:             sub.option,
	}
	group := NewGroup(sub.topic, sub.consumer)
	subScription.groups = append(subScription.groups, group)
	subScription.consumer_partition[sub.consumer] = sub.key
	return subScription
}
