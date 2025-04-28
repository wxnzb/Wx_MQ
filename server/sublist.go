package server

import (
	"sync"
)

// 一个Topic下面对应多个分区，通过map以分区名字将分区连接，没个分区需要由读写锁还有一个byte的队列
type Topic struct {
	Parts   map[string]*Partition    //key一般是分区的名字，value是分区的指针
	SubList map[string]*SubScription //key是消费者的名字，value是消费者订阅的信息
}

// 一个分区需要哪些信息？这个分区肯定要有自己的名字，这个分区肯定存了很多信息在队列，这里先用string类型，这个分区里面可定有很多消费者，每个消费者都应该知道他消费到哪条信息了，因此还需要偏移量
type Partition struct {
	sync.RWMutex
	key             string
	queue           []string
	consumer_offset map[string]int //这里string是某个消费者的唯一标识，int就是他消费到的偏移量
}

// 现在要我想，他就需要在哪个topic以及哪个分区，感觉没了
type SubScription struct {
	sync.RWMutex
	topic_name         string
	consumer_partition map[string]string //一个消费者可以属于多个消费者组
	groups             []*Group
}
