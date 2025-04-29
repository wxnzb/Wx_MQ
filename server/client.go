package server

import (
	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/client_operations"
	"context"
	"sync"
)

// 这是消费组，一个消费组可以消费多个topic
// 下面这个是消费者，它对应的string一般是他的ip_port,一个消费组里面当然有多个消费者
type Group struct {
	// topics    []*Topic
	// consumers map[string]*client_operations.Client
	rmu        sync.RWMutex
	topic_name string
	consumers  map[string]bool //这个到底是用来干啥的，为啥是bool
}

// 这是客户端的状态,state
const (
	ALIVE = "alive"
	DONE  = "done"
)

// 这是一个客户端，他需要他唯一的名字，他现在的状态，还有他订阅的东西，
type Client struct {
	rmu      sync.RWMutex
	name     string
	state    string
	subList  []*SubScription
	consumer client_operations.Client //现在还不太确定他是干什么的
}

// 向服务器发消息
func (cl *Client) Pub(msg string) bool {
	resp, err := cl.consumer.Pub(context.Background(), &api.PubRequest{
		Msg: msg,
	})
	if err != nil || resp.Ret == false {
		return false
	}
	return true
}
func NewGroup(topic, consumer string) *Group {
	group := &Group{
		rmu:        sync.RWMutex{},
		topic_name: topic,
	}
	group.consumers[consumer] = true
	return group
}
