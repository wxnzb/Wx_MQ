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
	Topic_Name     string
	Partition_Name string
	Msg            string
}

func (pro *Producer) Push(msg Message) error {
	pro.rmu.RLock()
	index := msg.Topic_Name + "_" + msg.Partition_Name
	ok := pro.Topic_Partition[index]
	pro.rmu.RUnlock()
	if ok {
		resp, err := pro.Cli.Push(context.Background(), &api.PushRequest{
			//这里报错的原因是ProducerId是int64不是string
			ProducerId: pro.Name,
			Topic:      msg.Topic_Name,
			Key:        msg.Partition_Name,
			Message:    msg.Msg,
		})
		if err == nil && resp.Ret {
			return nil
		} else {
			return errors.New("err!=nil or resp.Ret==false\n")
		}

	} else {
		return errors.New("this topic_partition is not has this producer")
	}
}
