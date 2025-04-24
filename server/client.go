package server

import "Wx_MQ/kitex_gen/api/client_operations"

// 这是消费组，一个消费组可以消费多个topic
// 下面这个是消费者，它对应的string一般是他的ip_port,一个消费组里面当然有多个消费者
type Group struct {
	topics    []*Topic
	consumers map[string]*client_operations.Client
}
