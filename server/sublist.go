package server

import (
	"sync"
)

// 一个Topic下面对应多个分区，通过map以分区名字将分区连接，没个分区需要由读写锁还有一个byte的队列
type Topic struct {
	Parts map[string]Partition
}
type Partition struct {
	sync.RWMutex
	key   string
	queue []Message
}
type Message struct {
	buf []byte
}
