package server

//他主要是用来管理消费者的，对应的就是client/clients/consumer.go
//服务器为什么需要对消费者进行管理呢？
//✅ 消费负载管理 + ✅ 消息可靠性保障 + ✅ 故障恢复控制
//cli-1 掉线，服务器记录它状态变为 DOWN；

// 此时新的 cli-3 加入，Broker 会尝试将 partition-0 的消费权交给 cli-3；

// 如果没有这个状态记录，Broker 就不知道哪些 partition 是“无人消费的”。

import (
	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/client_operations"
	"context"
	"errors"
	"sync"
	"time"
)

// 这是消费组，一个消费组可以消费多个topic
// 下面这个是消费者，它对应的string一般是他的ip_port,一个消费组里面当然有多个消费者
type Group struct {
	rmu        sync.RWMutex
	topic_name string
	consumers  map[string]bool //这个是看属于这个消费者组里面的消费者还活着没
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

// 这是消费者客户端的状态,state
const (
	ALIVE = "alive"
	DOWN  = "down"
)

// 这是一个消费者实体的内存镜像，又是消费者的客户端
// 他这个结构体既是含有这个消费者的信息，又有这个消费者的RPC客户端句柄
type ToConsumer struct {
	rmu      sync.RWMutex
	name     string //消费者的名字就是他的地址
	state    string
	subList  map[string]*SubScription //这里的string应该是SubScription的名字，topic_name+option类型
	consumer client_operations.Client //这个就是消费者进行消费的接口
	parts    map[string]*Parts        //消费者消费的分区
}

func NewToConsumer(ip_port string, consumer client_operations.Client) *ToConsumer {
	return &ToConsumer{
		rmu:      sync.RWMutex{},
		name:     ip_port,
		state:    ALIVE,
		subList:  make(map[string]*SubScription),
		consumer: consumer,
		parts:    make(map[string]*Parts),
	}
}

// 服务器作为消费者客户端向消费者推送消息
func (con *ToConsumer) Pub(msg string) bool {
	resp, err := con.consumer.Pub(context.Background(), &api.PubRequest{
		Msg: msg,
	})
	if err != nil || resp.Ret == false {
		return false
	}
	return true
}

// 服务器作为消费者客户端一直给消费者发送Pong看是否在线，直到他下线
func (con *ToConsumer) CheckConsumer() bool {
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
	con.state = DOWN
	con.rmu.Unlock()
	return true
}

// 消费者订阅消息，这个函数还没懂
func (con *ToConsumer) AddScription(sub *SubScription) {
	con.rmu.Lock()
	defer con.rmu.Unlock()
	con.subList[con.name] = sub //这句暂时还不理解，因为key的原因，与reduceScription的key是一样的
}

// ----------------------新加的感觉暂时没用上================
// 得到这个消费者的操作接口
func (con *ToConsumer) GetClient() client_operations.Client {
}

// 移除订阅，这个函数还没懂
func (con *ToConsumer) ReduceScription(sub_name string) {
	con.rmu.Lock()
	defer con.rmu.Unlock()
	delete(con.subList, sub_name)
}

// 将消费者标记为不活跃，现在是broker的消费者客户端不能接收到消费者ping的消息时，就找到的他所有的订阅，然后将他的所有订阅的组里都标记为不活跃，为什么不直接删除？？
func (g *Group) DownConsumer(consumer_name string) {
	g.rmu.Lock()
	//这里为什么不直接写成g.consumers[consumer_name] = false，那要是你随便写一个消费者组里面就不存在的消费者的名字呢？对把
	if _, ok := g.consumers[consumer_name]; ok {
		g.consumers[consumer_name] = false
	}
	g.rmu.Unlock()
}

// 感觉这个还没用上
// 删除消费者
func (g *Group) DeleteConsumer(consumer_name string) {
	g.rmu.Lock()
	//这里为什么不直接写成g.consumers[consumer_name] = false，不活跃和删除不是一个概念
	if _, ok := g.consumers[consumer_name]; ok {
		delete(g.consumers, consumer_name)
	}
	g.rmu.Unlock()
}

// 要是消费者重新向服务端发送消息证明他还活着，恢复消费者
func (g *Group) RecoverConsumer(consumer_ipname string) {
	g.rmu.Lock()
	defer g.rmu.Unlock()
	_, ok := g.consumers[consumer_ipname]
	//
	if ok {
		if g.consumers[consumer_ipname] {
			errors.New("consumer is alive")
		} else {
			g.consumers[consumer_ipname] = true
		}

	} else {
		errors.New("no such consumer")
	}
}

// Parts
type Parts struct {
}
