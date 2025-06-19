package clients

import (
	"Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/server_operations"
	"context"
	"errors"
	"sync"
)

type Producer struct {
	Cli             server_operations.Client //生产者需要调用的接口
	rmu             sync.RWMutex
	Name            string
	Topic_Partition map[string]bool //这个topic的Partition是否是这个生产者负责
}
type Message struct {
	Topic     string
	Partition string
	Msg       string
}

func (pro *Producer) Push(msg Message) error {
	pro.rmu.RLock()
	index := msg.Topic + "_" + msg.Partition
	if _, ok := pro.Topic_Partition[index]; ok {
		pro.rmu.RUnlock()
		resp, err := pro.Cli.Push(context.Background(), &api.PushRequest{
			ProducerId: pro.name,
			Topic:      msg.Topic,
			Key:        msg.Partition,
			Message:    msg.Msg,
		})

	} else {
		return errors.New("this topic_partition is not has this producer")
	}
}
