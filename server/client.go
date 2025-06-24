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
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	client2 "github.com/cloudwego/kitex/client"
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
	parts    map[string]*Part         //消费者消费的分区
}

// 那么这个也换一个版本
//
//	func NewToConsumer(ip_port string, consumer client_operations.Client) *ToConsumer {
//		return &ToConsumer{
//			rmu:      sync.RWMutex{},
//			name:     ip_port,
//			state:    ALIVE,
//			subList:  make(map[string]*SubScription),
//			consumer: consumer,
//			parts:    make(map[string]*Part),
//		}
//	}
func NewToConsumer(ip_port string) (*ToConsumer, error) {
	client, err := client_operations.NewClient("clients", client2.WithHostPorts(ip_port))
	if err != nil {
		DEBUG(dERROR, "NewClient err:%v", err)
		return nil, err
	}
	return &ToConsumer{
		rmu:      sync.RWMutex{},
		name:     ip_port,
		state:    ALIVE,
		subList:  make(map[string]*SubScription),
		consumer: client,
		parts:    make(map[string]*Part),
	}, nil
}

//这个也没了？？？
// // 服务器作为消费者客户端向消费者推送消息
// func (con *ToConsumer) Pub(msg string) bool {
// 	resp, err := con.consumer.Pub(context.Background(), &api.PubRequest{
// 		Msg: msg,
// 	})
// 	if err != nil || resp.Ret == false {
// 		return false
// 	}
// 	return true
// }

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

// 新加函数：查看这个消费者是否订阅了这个sub
func (con *ToConsumer) CheckSubscription(sub_name string) bool {
	con.rmu.RLock()
	defer con.rmu.RUnlock()
	_, ok := con.subList[sub_name]
	return ok
}

// ----------------------新加的感觉暂时没用上================
// 得到这个消费者的操作接口
func (con *ToConsumer) GetToConsumer() *client_operations.Client {
	con.rmu.RLock()
	defer con.rmu.Unlock()
	return &con.consumer
}

// 移除订阅
func (con *ToConsumer) ReduceScription(sub_name string) {
	con.rmu.Lock()
	defer con.rmu.Unlock()
	delete(con.subList, sub_name)
}

// 将消费者标记为不活跃，现在是broker的消费者客户端不能接收到消费者ping的消息时，就找到的他所有的订阅，然后将他的所有订阅的组里都标记为不活跃，为什么不直接删除？？
func (g *Group) DownConsumer(consumer_ipname string) {
	g.rmu.Lock()
	//这里为什么不直接写成g.consumers[consumer_name] = false，那要是你随便写一个消费者组里面就不存在的消费者的名字呢？对把
	if _, ok := g.consumers[consumer_ipname]; ok {
		g.consumers[consumer_ipname] = false
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

// 一个block包含一个node和多个messages[多批消息，每批消息都有自己的index]
// Part，这里的part应该是某个消费者/消费者组对应的分区状态
type Part struct {
	rmu            sync.RWMutex
	topic_name     string
	partition_name string
	to_consumers   map[string]*client_operations.Client //这里的string应该是consumer的ip_port
	option         int8
	state          string
	//文件
	file        *File
	fd          os.File //感觉这个暂时不需要，可以由上面那个推出来
	lastIndex   int64   //被所有订阅人他的消费者消费的消息的索引
	blockOffset int64   //消费的这批消息的开始块位置
	//读取瓷盘中的消息存入的地方
	buffer_node  map[int64]NodeData
	buffer_msgs  map[int64][]Message
	startIndex   int64
	endIndex     int64
	consumer_ack (chan ConsumerAck)
	block_status map[int64]string
}

func NewPart(partitionInitInfo PartitionInitInfo, toConsumers map[string]*client_operations.Client, file *File) *Part {
	return &Part{
		rmu:            sync.RWMutex{},
		topic_name:     partitionInitInfo.topic,
		partition_name: partitionInitInfo.partition,
		option:         partitionInitInfo.option,
		state:          ALIVE,
		file:           file,
		to_consumers:   toConsumers,
		buffer_node:    make(map[int64]NodeData),
		buffer_msgs:    make(map[int64][]Message),
		consumer_ack:   make(chan ConsumerAck),
		block_status:   make(map[int64]string),
	}
}
func (con *ToConsumer) StartPart(req PartitionInitInfo, toConsumers map[string]*client_operations.Client, file *File) {
	con.rmu.Lock()
	defer con.rmu.Unlock()
	part, ok := con.parts[req.topic]
	if !ok {
		//func NewPart(partitionInitInfo PartitionInitInfo, toConsumers map[string]*client_operations.Client, file *File) *Part
		//要是没有就要新建一个
		part = NewPart(req, toConsumers, file)
		con.parts[req.topic] = part
	}
	part.Start()
}

// 这个函数主要做的事情有：
// 1：读取文件的消息存到part结构体的buffer_node和buffer_mags中
// 2：开启一个携程接收消费者发送过来的ack
// 3:开启携程向消费者发送消息
func (p *Part) Start() {
	//打开一个文件
	p.fd = *p.file.OpenFile()
	//根据index找到偏移
	//p.fileOffset=p.file.FindOffset(p.lastIndex)
	var err error
	//这里得到的是这一块消息的偏移，他无法得到这一批消息的偏移，但是先在感觉这句根本就没有用上阿，为什么要放在这里？？
	p.blockOffset, err = p.file.FindOffset(&p.fd, p.lastIndex)
	if err != nil {

	}
	//开始从磁盘读取5个快放在buffer中
	for i := 0; i < 5; i++ {
		err = p.AddBlock()
	}
	//开启一个携程接收消费者发送过来的ack
	go p.GetDone()
	//现在循环发送消息给消费者
	for {
		p.rmu.RLock()
		if p.state == DOWN {
			break
		}
		p.rmu.RUnlock()
		p.rmu.RLock()
		for name, toConsumer := range p.to_consumers {
			go p.SendOneBlock(name, toConsumer, p.startIndex)
		}
	}
	p.rmu.RUnlock()
}

const (
	//这个是ConsumerAck里的err
	OK      = "ok"
	TIMEOUT = "timeout"
	//这个是part里的block_status
	HADDO      = "haddo"
	NOTDO      = "notdo"
	DOING      = "doing"
	FALL_TIMES = 3
)

// 消费者在收到消息后返回的ack信息
type ConsumerAck struct {
	start_index int64
	err         string
	//使得part删除这个消费者的时候可以找到
	name string
	//这个我还不知道有什么用
	to_consumer *client_operations.Client
}

// 走的是负载均衡，每个块只能被一个消费者消费
// |node-1...50|node-51...120|node-121...200|这就是三个块，这里的1.50等都是这批消息的索引
func (p *Part) GetDone() {
	for {
		select {
		case finish := <-p.consumer_ack:

			if finish.err == OK {
				err := p.AddBlock()
				if err != nil {

				}
				p.rmu.Lock()
				p.block_status[finish.start_index] = HADDO
				//首先这里用for循环的原因：p.startIndex他和收到的finish.start_index不一定一样，比如先在发送了101,102消息，但是先收到102,但是现在p.startIndex=101,发现还不是ok，就break,就意思是你发送了很多块，只又收到的前面都已经被处理了才会前进p.startIndex
				for {
					//因为已经发送成功，现在要删除里面的信息
					if p.block_status[p.startIndex] == HADDO {
						delete(p.block_status, p.startIndex)
						//加这一句的原因是，要是不加，你都把delete(p.buffer_node, p.startIndex)这个删了，在哪里去找,所以要提前保存
						node := p.buffer_node[p.startIndex]
						delete(p.buffer_node, p.startIndex)
						delete(p.buffer_msgs, p.startIndex)
						//p.startIndex = p.buffer_node[p.startIndex].End_index + 1
						p.startIndex = node.End_index + 1
					} else {
						break
					}
				}
				p.rmu.Unlock()
			}
			if finish.err == TIMEOUT {
				//消费者可能掉线了，删了他，亨，不给他发了
				p.rmu.Lock()
				delete(p.to_consumers, finish.name)
				p.rmu.Unlock()
			}
		}
	}
}

// 这里我有个问题，他这个offset在PartStart()出现赋值，但是他个块不是已经读过了吗？？？
// 讲文件里的block存到内存里面
func (p *Part) AddBlock() error {
	//打开文件，思考从哪里开始读取
	node, msgs, err := p.file.ReadFile(&p.fd, p.blockOffset)
	if err != nil {
		return err
	}
	//将读取到的块存在p.buffer_node和p.buffer_msgs中
	p.rmu.Lock()
	defer p.rmu.Unlock()
	//为啥这里就只用把start_index存进去就行了，不用关end_index吗？？
	p.buffer_node[node.Start_index] = node
	p.buffer_msgs[node.Start_index] = msgs
	p.block_status[node.Start_index] = NOTDO
	p.blockOffset += int64(NODE_SIZE + node.Size)
	p.endIndex = node.End_index
	return nil
}

func (p *Part) SendOneBlock(name string, toConsumer *client_operations.Client, startIndex int64) {
	//为了防止他开始让发的已经发过了，因此要在外面在加一层for循环
	for {
		if p.block_status[startIndex] == NOTDO {
			node := p.buffer_node[startIndex]
			msgs := p.buffer_msgs[startIndex]
			//因为Pub是byte刘，这里要进行转化
			msgs_data := make([]byte, node.Size)
			var err error
			msgs_data, err = json.Marshal(msgs)
			num := 0
			p.rmu.Lock()
			p.block_status[startIndex] = DOING
			p.rmu.Unlock()
			//循环发送
			for {
				err = p.Pub(toConsumer, node, msgs_data)
				if err != nil {
					num++
					if num >= FALL_TIMES {
						p.rmu.Lock()
						p.block_status[startIndex] = NOTDO
						p.rmu.Unlock()
						p.consumer_ack <- ConsumerAck{
							start_index: startIndex,
							err:         TIMEOUT,
							name:        name,
							to_consumer: toConsumer,
						}
						break
					}
				} else {
					//要是发送成功了，不需要在这里确认，他分成两步，有先将占说明这个消息正在发送，那么不会再发给其他的消费者，然后要是发送成功并且消费者返回ack,GetDone会将他有doing修改成haddo
					//p.block_status[startIndex] = HADDO
					p.consumer_ack <- ConsumerAck{
						start_index: startIndex,
						err:         OK,
						name:        name,
						to_consumer: toConsumer,
					}
					break
				}

			}
			break
		} else {
			startIndex += p.buffer_node[startIndex].End_index + 1
		}
	}
}

// func (p *kClient) Pub(ctx context.Context, req *api.PubRequest) (r *api.PubResponse, err error)
func (p *Part) Pub(toConsumer *client_operations.Client, node NodeData, msgs_data []byte) error {
	resp, err := (*toConsumer).Pub(context.Background(),
		&api.PubRequest{
			TopicName:     p.topic_name,
			PartitionName: p.partition_name,
			StartIndex:    node.Start_index,
			EndIndex:      node.End_index,
			Msg:           msgs_data,
		})
	if err != nil {

	}
	if resp.Ret == false {

	}
	return nil
}

// 这个还没用上
func (p *Part) ClosePart() {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	p.state = DOWN
}
