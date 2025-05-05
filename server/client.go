package server

import (
	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/client_operations"
	"context"
	"sync"
	"time"
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

// 这是一个消费者实体，他需要他唯一的名字，他现在的状态，还有他订阅的东西，
type Consumer struct {
	rmu     sync.RWMutex
	name    string
	state   string
	subList map[string]*SubScription //我就觉得应该加上string//客户端订阅列表,若consumer关闭则遍历这些订阅并修改
	// 疑问：这里的string就是consuemr里面的name？？
	consumer client_operations.Client //现在还不太确定他是干什么的
}

// 向服务器发消息
func (con *Consumer) Pub(msg string) bool {
	resp, err := con.consumer.Pub(context.Background(), &api.PubRequest{
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
func (g *Group) AddConsumer(con_name string) {
	g.rmu.Lock()
	defer g.rmu.Unlock()
	g.consumers[con_name] = true
}
func (con *Consumer) AddScription(sub *SubScription) {
	con.rmu.Lock()
	defer con.rmu.Unlock()
	con.subList[con.name] = sub
}
func NewConsumer(ip_port string, consumer client_operations.Client) *Consumer {
	return &Consumer{
		rmu:      sync.RWMutex{},
		name:     ip_port,
		state:    ALIVE,
		subList:  make(map[string]*SubScription),
		consumer: consumer,
	}
}
func (con *Consumer) CheckConsumer() bool {
	con.rmu = sync.RWMutex{}
	for {
		//Ping 请求是发送给 Broker 服务端的，用于检测消费者是否在线
		resp, err := con.consumer.Pingpong(context.Background(), &api.PingpongRequest{Ping: true})
		if resp.Pong == false || err != nil {
			break
		}
		time.Sleep(time.Second)
	}
	con.rmu.Lock()
	con.state = DONE
	con.rmu.Unlock()
	return true
}

// 将消费者标记为不活跃，现在是当消费者不能发送ping的时候就找到的他所有的订阅，然后将他的所有订阅的组里都标记为不活跃，为什么不直接删除？？
func (g *Group) DownConsumer(consumer_name string) {
	g.rmu.Lock()
	//这里为什么不直接写成g.consumers[consumer_name] = false
	if _, ok := g.consumers[consumer_name]; ok {
		g.consumers[consumer_name] = false
	}
	g.rmu.Unlock()
}

// 感觉这个还没用上
// 删除消费者
func (g *Group) DeleteConsumer(consumer_name string) {
	g.rmu.Lock()
	//这里为什么不直接写成g.consumers[consumer_name] = false
	if _, ok := g.consumers[consumer_name]; ok {
		delete(g.consumers, consumer_name)
	}
	g.rmu.Unlock()
}
