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
	"Wx_MQ/kitex_gen/api/zkserver_operations"
	"Wx_MQ/logger"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	client2 "github.com/cloudwego/kitex/client"
)

// 这是消费者客户端的状态,state
const (
	ALIVE       = "alive"
	CLOSE       = "close"
	DOWN        = "down"
	TOUT        = 60 * 10
	UPDATENUM   = 10
	ErrHadStart = "this partition had Start"
)

// 这是一个消费者实体的内存镜像，又是消费者的客户端
// 他这个结构体既是含有这个消费者的信息，又有这个消费者的RPC客户端句柄
// 现在这个消费者客户端有消费者：名字，状态，操作消费者的接口，订阅列表
type ToConsumer struct {
	rmu      sync.RWMutex
	name     string //消费者的名字就是他的地址
	state    string
	subList  map[string]*SubScription //这里的string应该是SubScription的名字，topic_name+option类型
	consumer client_operations.Client //这个就是消费者进行消费的接口
}

func NewToConsumer(ip_port string) (*ToConsumer, error) {
	client, err := client_operations.NewClient("clients", client2.WithHostPorts(ip_port))
	if err != nil {
		logger.DEBUG(logger.DError, "connect client failed\n")
		return nil, err
	}
	logger.DEBUG(logger.DLog, "connect consumer server successful\n")
	return &ToConsumer{
		rmu:      sync.RWMutex{},
		name:     ip_port,
		state:    ALIVE,
		subList:  make(map[string]*SubScription),
		consumer: client,
	}, nil
}

// 服务器作为消费者客户端一直给消费者发送Pong看是否在线，直到他下线
func (con *ToConsumer) CheckConsumer() bool {
	con.rmu = sync.RWMutex{}
	for {
		//Ping 请求是发送给consumer服务端，用于检测消费者是否在线
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

// 查看这个消费者是否订阅了这个sub
func (con *ToConsumer) CheckSubscription(sub_name string) bool {
	con.rmu.RLock()
	defer con.rmu.RUnlock()
	_, ok := con.subList[sub_name] //由此可知：SubScription这个结构体中一定要有他自己的名字才能连接起来
	return ok
}

// 消费者订阅消息
func (con *ToConsumer) AddScription(sub *SubScription) {
	con.rmu.Lock()
	defer con.rmu.Unlock()
	con.subList[sub.name] = sub
}

// 移除订阅
func (con *ToConsumer) ReduceScription(sub_name string) {
	con.rmu.Lock()
	defer con.rmu.Unlock()
	delete(con.subList, sub_name)
}

// 得到这个消费者的操作接口
func (con *ToConsumer) GetToConsumer() *client_operations.Client {
	con.rmu.RLock()
	defer con.rmu.Unlock()
	return &con.consumer //这里为什么不直接将ToConsumer操作消费者的接口写成指针形式呢？？？
}

// 获取消费者状态
func (con *ToConsumer) GetState() string {
	con.rmu.RLock()
	defer con.rmu.Unlock()
	return con.state
}

// 获取消费者订阅的Sub
func (con *ToConsumer) GetSub(sub_name string) *SubScription {
	con.rmu.RLock()
	defer con.rmu.RUnlock()
	//先这样做吧，这里应该先判断一下她存在不？？
	ok := con.CheckSubscription(sub_name)
	if !ok {
		DEBUG(dError, "no such sub:%s", sub_name)
	}
	return con.subList[sub_name]
}

// --------------------------------------------------------------------------------
// 这是消费组，一个消费组可以消费多个topic
// 消费者组需要有他所消费的topic_name,还需要有他里面的消费者以及存活情况
type Group struct {
	rmu        sync.RWMutex
	topic_name string
	consumers  map[string]bool //[ip_port]isornoAlive
}

// 创建一个消费者组那里面肯定需要至少有一个消费者，因此他需要参数topic_name和consumer_name
func NewGroup(topic, consumer string) *Group {
	group := &Group{
		rmu:        sync.RWMutex{},
		topic_name: topic,
		consumers:  make(map[string]bool),
	}
	group.consumers[consumer] = true
	return group
}
func (g *Group) AddConsumer(con_name string) error {
	g.rmu.Lock()
	defer g.rmu.Unlock()
	if _, ok := g.consumers[con_name]; ok {
		return errors.New("consumer already exist")
	}
	g.consumers[con_name] = true
	return nil
}

// 删除消费者
func (g *Group) DeleteConsumer(consumer_name string) error {
	g.rmu.Lock()
	defer g.rmu.Unlock()
	//这里为什么不直接写成g.consumers[consumer_name] = false，不活跃和删除不是一个概念
	if _, ok := g.consumers[consumer_name]; ok {
		delete(g.consumers, consumer_name)
	} else {
		return errors.New("consumer not exist")
	}
	return nil
}

// 将消费者标记为不活跃，现在是broker的消费者客户端不能接收到消费者ping的消息时，就找到的他所有的订阅，然后将他的所有订阅的组里都标记为不活跃，为什么不直接删除？？
func (g *Group) DownConsumer(consumer_name string) error {
	g.rmu.Lock()
	defer g.rmu.Unlock()
	alive, ok := g.consumers[consumer_name]
	if !ok {
		return errors.New("no such consumer")
	}
	if !alive {
		return errors.New("consumer 本来就是 down的")
	}
	g.consumers[consumer_name] = false
	return nil
}

// 要是消费者重新向服务端发送消息证明他还活着，恢复消费者
func (g *Group) RecoverConsumer(consumer_name string) error {
	g.rmu.Lock()
	defer g.rmu.Unlock()
	alive, ok := g.consumers[consumer_name]
	if !ok {
		return errors.New("no such consumer")
	}
	if alive {
		return errors.New("consumer 本来就是 alive的")
	}
	g.consumers[consumer_name] = true
	return nil
}

// -------------------------------------------------------------------
// 一个block包含一个node和多个messages多批消息，每批消息都有自己的index]
// Part，这里的part应该是某个消费者/消费者组对应的分区状态
// 想一下一个分区都需要什么：分区对应主题的名字，分区的名字，状态，哪些消费者订阅了该分区，分区从哪个文件拿数据，并存到自己的buffer里面，
// 当把buffer里面的消息发送过给消费者的时候，将消费者返回的信息存到consumer_ack里面
type Part struct {
	rmu            sync.RWMutex
	topic_name     string
	partition_name string
	to_consumers   map[string]*client_operations.Client //这里的string应该是consumer的ip_port
	option         int8                                 //------------------------------------------------------
	state          string
	//文件
	file        *File
	fd          os.File //感觉这个暂时不需要，可以由上面那个推出来
	lastIndex   int64   //被所有订阅人他的消费者消费的消息的索引--------------------------------------
	blockOffset int64   //消费的这批消息的开始块位置------------------------------------------------
	//读取瓷盘中的消息存入的地方
	buffer_node  map[int64]NodeData
	buffer_msgs  map[int64][]Message
	startIndex   int64 //-----------------------------------------------------------------------
	endIndex     int64 //-----------------------------------------------------------------------
	consumer_ack (chan ConsumerAck)
	block_status map[int64]string //------------------------------------------------------------
	zkclient     *zkserver_operations.Client
}

func NewPart(in Info, file *File, zkclient *zkserver_operations.Client) *Part {
	return &Part{
		rmu:            sync.RWMutex{},
		topic_name:     in.topic,
		partition_name: in.partition,
		option:         in.option,
		state:          DOWN,
		file:           file,
		to_consumers:   make(map[string]*client_operations.Client),
		buffer_node:    make(map[int64]NodeData),
		buffer_msgs:    make(map[int64][]Message),
		//consumer_ack:   make(chan ConsumerAck),
		block_status: make(map[int64]string),
		lastIndex:    in.offset,
		zkclient:     zkclient,
	}

}

// func (con *ToConsumer) StartPart(req PartitionInitInfo, toConsumers map[string]*client_operations.Client, file *File) {
// 	con.rmu.Lock()
// 	defer con.rmu.Unlock()
// 	part, ok := con.parts[req.topic]
// 	if !ok {
// 		//func NewPart(partitionInitInfo PartitionInitInfo, toConsumers map[string]*client_operations.Client, file *File) *Part
// 		//要是没有就要新建一个
// 		part = NewPart(req, toConsumers, file)
// 		con.parts[req.topic] = part
// 	}
// 	part.Start()
// }

// 这个函数主要做的事情有：
// 1：读取文件的消息存到part结构体的buffer_node和buffer_mags中
// 2：开启一个携程接收消费者发送过来的ack
// 3:开启携程向消费者发送消息
func (p *Part) Start(close chan *Part) {
	//打开一个文件
	p.fd = *p.file.OpenFileRead()
	//根据index找到偏移
	//p.fileOffset=p.file.FindOffset(p.lastIndex)
	var err error
	//这里得到的是这一块消息的偏移，他无法得到这一批消息的偏移，但是先在感觉这句根本就没有用上阿，为什么要放在这里？？
	p.blockOffset, err = p.file.FindOffset(&p.fd, p.lastIndex)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	//开始从磁盘读取5个快放在buffer中
	for i := 0; i < BUFF_NUM; i++ {
		err = p.AddBlock()
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}
	}
	//开启一个携程接收消费者发送过来的ack
	go p.GetDone(close)
	//现在循环发送消息给消费者
	p.rmu.Lock()

	if p.state == DOWN {
		p.state = ALIVE
	} else {
		p.rmu.Unlock()
		logger.DEBUG(logger.DError, "the part is ALIVE in before this start\n")
		return
	}
	for name, toConsumer := range p.to_consumers {
		go p.SendOneBlock(name, toConsumer)
	}
	p.rmu.Unlock()
}

const (
	//这个是ConsumerAck里的err
	OK       = "ok"
	STIMEOUT = "timeout"
	//这个是part里的block_status
	HADDO      = "haddo"
	NOTDO      = "notdo"
	DOING      = "doing"
	FALL_TIMES = 3
	BUFF_NUM   = 5
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
// 需要修改成可主动关闭模式
func (p *Part) GetDone(close chan *Part) {
	num := 0
	for {
		select {
		case finish := <-p.consumer_ack:

			if finish.err == OK {
				//要是到达这个书就要更新zookeeper中的offset
				num++
				err := p.AddBlock()
				if err != nil {
					DEBUG(dError, err.Error())
				}
				if p.file.filePath != p.partition_name+"NowBlock.txt" && err == errors.New("read All file,don't find this index") {
					p.state = DOWN
				}
				//这里len(p.block_status)还需要思考
				if p.state == DOWN && len(p.block_status) == 0 {
					p.rmu.Unlock()
					close <- p
					return
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
						(*p.zkclient).UpdateOffset(context.Background(), &api.UpdateOffsetRequest{
							Topic:  p.topic_name,
							Part:   p.partition_name,
							Offset: p.startIndex,
						})
					} else {
						break
					}
				}
				p.rmu.Unlock()
			}
			if finish.err == STIMEOUT {
				//消费者可能掉线了，删了他，亨，不给他发了
				p.rmu.Lock()
				delete(p.to_consumers, finish.name)
				p.rmu.Unlock()
			}
		case <-time.After(time.Second * TOUT): //超时
			close <- p
			return
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

func (p *Part) SendOneBlock(name string, toConsumer *client_operations.Client) {
	//为了防止他开始让发的已经发过了，因此要在外面在加一层for循环
	var startIndex int64
	startIndex = 0
	num := 0
	for {
		p.rmu.Lock()
		if startIndex == 0 {
			startIndex = p.startIndex
		}
		//这个消费者不是这个分区负责的
		if _, ok := p.to_consumers[name]; !ok {
			p.rmu.Unlock()
			return
		}
		if p.block_status[startIndex] == NOTDO {
			node := p.buffer_node[startIndex]
			msgs := p.buffer_msgs[startIndex]
			//表示这个块正在被处理
			p.block_status[startIndex] = DOING
			p.rmu.Unlock()
			//因为Pub是byte刘，这里要进行转化
			msgs_data := make([]byte, node.Size)
			var err error
			msgs_data, err = json.Marshal(msgs)
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
							err:         STIMEOUT,
							name:        name,
							to_consumer: toConsumer,
						}
						break
					}
				} else {
					//要是发送成功了，不需要在这里确认，他分成两步，有先将占说明这个消息正在发送，那么不会再发给其他的消费者，然后要是发送成功并且消费者返回ack,GetDone会将他有doing修改成haddo
					//p.block_status[startIndex] = HADDO
					p.consumer_ack <- ConsumerAck{
						//这个写发和上面那个有区别吗
						start_index: node.Start_index,
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

// // 这个还没用上
// func (p *Part) ClosePart() {
// 	p.rmu.Lock()
// 	defer p.rmu.Unlock()
// 	p.state = DOWN
// }

func (p *Part) UpdateCons(confPartToCons []string, confCons map[string]*client_operations.Client) {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	reduce, add := CheckChangeCon(p.to_consumers, confPartToCons)
	for _, name := range reduce {
		delete(p.to_consumers, name)
	}
	for _, name := range add {
		p.to_consumers[name] = confCons[name]
		//开启携程,发送消息
		go p.SendOneBlock(name, confCons[name])
	}
}

type Node struct {
	topic_name     string
	partition_name string
	option         int8
	file           *File

	fd os.File
	//消息偏移
	offset int64
	//上次读去消息的索引
	start_index int64
}

func NewNode(in Info, file *File) *Node {
	no := &Node{
		topic_name:     in.topic,
		partition_name: in.partition,
		option:         in.option,
		file:           file,
	}
	no.fd = *no.file.OpenFileRead()
	no.offset = -1
	return no
}

// 这样读取数据有两个问题:
// 2:假设每个node中的消息有5条,现在我第一个对这,但是size也就是我向度的消息数量16条,在读到5条时,nums < int(in.size)还不满足,那么会多读4条消息
//
//1:node(0) 1  2  3 4 5,但是我从3开始读,因为他不是node的偏移,他会通过find函数找到0,那么就会读到这个之前的消息
func (nod *Node) ReadMSGS(in Info) (MSGS, error) {
	var err error
	//这里判断nod.start_index != in.offset,因为start_index是上一次消费者消费时node的开始,比较,要是不一样就通过find函数找到对应的in.offset的node偏移
	if nod.offset == -1 || nod.start_index != in.offset {
		nod.offset, err = nod.file.FindOffset(&nod.fd, in.offset)
		if err != nil {
			return MSGS{}, nil
		}
	}
	nums := 0
	var msgs MSGS
	for nums < int(in.size) {
		node, msg, err := nod.file.ReadBytes(&nod.fd, nod.offset)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return MSGS{}, err
			}
		}
		if nums == 0 {
			msgs.start_index = node.Start_index //start_index是这个开始node起始偏移量,假设你以工读了三个node里面的消息,他的这个还是第一个读的node的偏移
		}
		nums += int(node.Size) //nums代表几条消息
		nod.offset += int64(NODE_SIZE) + int64(node.Size)
		msgs.size = int8(nums)
		msgs.array = append(msgs.array, msg...)
		msgs.end_index = node.End_index
	}
	return msgs, nil
}
func (f *File) ReadBytes(file *os.File, offset int64) (NodeData, []byte, error) {
	f.rmu.RLock()
	defer f.rmu.RUnlock()
	//读取节点元数据
	node_data := make([]byte, NODE_SIZE)
	size, err := file.ReadAt(node_data, offset)
	if err != nil {
		if err == io.EOF {
			logger.DEBUG(logger.DLeader, "EOF reached while reading node metadata at offset %d", offset)
			return NodeData{}, nil, fmt.Errorf("node metadata EOF at offset %d", offset)
		}
		logger.DEBUG(logger.DLeader, "Read error: %v", err)
		return NodeData{}, nil, fmt.Errorf("failed to read node metadata: %w", err)
	}
	if size != NODE_SIZE {
		logger.DEBUG(logger.DLog2, "Incomplete read: got %d bytes,expected %d\n", size, NODE_SIZE)
		return NodeData{}, nil, fmt.Errorf("incomplete node read: expected %d bytes, got %d", NODE_SIZE, size)
	}
	//解析节点元数据
	var node NodeData
	if err := binary.Read(bytes.NewReader(node_data), binary.LittleEndian, &node); err != nil {
		logger.DEBUG(logger.DLog2, "Failed to parse node metadata: %v", err)
		return NodeData{}, nil, fmt.Errorf("failed to parse node metadata: %w", err)
	}
	//读取节点数据
	msg_data := make([]byte, node.Size)
	offset += int64(f.node_size)
	size, err = file.ReadAt(msg_data, offset)
	//检查节点数据读取结果
	if err != nil {
		if err == io.EOF {
			logger.DEBUG(logger.DLeader, "EOF reached while reading node data at offset %d", offset+int64(NODE_SIZE))
			return node, nil, fmt.Errorf("node data EOF at offset %d", offset+int64(NODE_SIZE))
		}
		logger.DEBUG(logger.DLeader, "Read error: %v", err)
		return NodeData{}, nil, fmt.Errorf("failed to read node data: %w", err)
	}
	if size != node.Size {
		logger.DEBUG(logger.DLeader, "Incomplete read: got %d bytes,expected %d\n", size, node.Size)
		return node, nil, fmt.Errorf("incomplete node data read: expected %d bytes, got %d", node.Size, size)
	}
	return node, msg_data, nil
}
