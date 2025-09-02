package clients

//一个消费者客户端可以消费多个broker里面的topic-partition的消息
import (
	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/client_operations"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"Wx_MQ/kitex_gen/api/server_operations"

	"Wx_MQ/kitex_gen/api/zkserver_operations"

	"github.com/cloudwego/kitex/client"
	server "github.com/cloudwego/kitex/server"
)

// 这里应该还需要实现一个pull函数
type Consumer struct {
	rmu sync.RWMutex
	//这里是大写是因为其他地方要用到
	// Cli   server_operations.Client //到broker的RPC客户端句柄
	//现在怎么感觉port和Name是一样的作用?
	Name  string
	state string
	//------------------
	//这个应该是为pingpong和pub做准备的，他是这两个的服务端
	port string ///这个port是给broker注册的
	srv  server.Server
	//------------------
	zkBrokerCli zkserver_operations.Client
	Brokers     map[string]server_operations.Client
}
type Parts struct {
	PartKeys []PartKey `json:"partkeys`
}

func NewConsumer(zkBrokerIpport, name, port string) (*Consumer, error) {
	c := &Consumer{
		rmu:     sync.RWMutex{},
		Name:    name,
		state:   "alive",
		port:    port,
		Brokers: make(map[string]server_operations.Client),
	}
	var err error
	c.zkBrokerCli, err = zkserver_operations.NewClient(name, client.WithHostPorts(zkBrokerIpport))
	return c, err
}
func (con *Consumer) GetState() string {
	con.rmu.RLock()
	defer con.rmu.RUnlock()
	return con.state
}
func (con *Consumer) Stop() {
	con.srv.Stop()
}
func (con *Consumer) Down() {
	con.rmu.Lock()
	defer con.rmu.Unlock()
	con.state = "down"
}

// -----------------------------------------------
// 这里是broker发送给消费者客户端是他们的反应
// 下面这个是启动一个rpc服务器，broker服务器测试消息推送功能或探活功能就是通过和他交流的
// 下面这两个会被执行的条件就是broker发送给消费者客户端的消息
// 但是我还是感觉很奇怪，为啥他前面是（con *Consumer)??
func (con *Consumer) Pub(ctx context.Context, req *api.PubRequest) (r *api.PubResponse, err error) {
	fmt.Println(req.Msg)
	return &api.PubResponse{
		Ret: true,
	}, nil
}

func (con *Consumer) Pingpong(ctx context.Context, req *api.PingpongRequest) (r *api.PingpongResponse, err error) {
	fmt.Println("Pingpong")
	return &api.PingpongResponse{
		Pong: true,
	}, nil
}

// [server]----对应server/client.go中的NewToConsumer
func (con *Consumer) Start_server() {
	//返回的是*TCPAddr,他实现了Network函数和String函数，也就是实现了net.Addr接口
	addr, _ := net.ResolveTCPAddr("tcp", con.port)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))
	//第一个参数要实现api.Client_Operations这个接口
	con.srv = client_operations.NewServer(new(Consumer), opts...)
	err := con.srv.Run()
	if err != nil {
		println(err.Error())
	}
}

// ------------------------------------------------
// 像broker注册自己并创建这个消费者对应的客户端，就可以像消费者发送pingpong和pub了
func (con *Consumer) RegisterSelf(port string, cli *server_operations.Client) error {
	resp, err := (*cli).Info(context.Background(), &api.InfoRequest{
		IpPort: port,
	})
	if err != nil {
		fmt.Print(resp)
	}
	return err
}

// 想zkserver订阅topic或者partition
func (c *Consumer) SubScription(topic_name, partition_name string, option int8) (err error) {
	//为了避免数据竞争，这里需要加锁
	c.rmu.Lock()
	zk := c.zkBrokerCli
	c.rmu.Unlock()
	resp, err := zk.Sub(context.Background(), &api.SubRequest{
		Consumer: c.Name,
		Topic:    topic_name,
		Key:      partition_name,
		Option:   option,
	})
	if err != nil || resp.Ret == false {
		return err
	}
	return nil
}

// 给后续函数共同使用
type InfoReq struct {
	Topic     string
	Partition string
	Offset    int64
	Option    int8
	Bufs      map[int64]*api.PubRequest
	Cli       server_operations.Client
	Size      int8
}

func NewInfoReq(topic, partition string, offset int64) InfoReq {
	return InfoReq{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Bufs:      make(map[int64]*api.PubRequest),
	}
}

// 下面这两个很像，第一个是对单个partition进行拉取，第二个是对多个partition进行拉取
// 目前的疑问：第一个函数的Cli是从哪来的
// 这个消息发送后，broker就会开始向这个消费者发送消息(pub)
func (con *Consumer) StartGet(info InfoReq) (parts []PartKey, ret string, err error) {
	// ret := ""
	// req := api.InfoGetRequest{
	// 	Cli_Name:       con.Name,
	// 	Topic_Name:     info.Topic,
	// 	Partition_Name: info.Partition,
	// 	Offset:         info.Offset,
	// 	Option:         info.Option,
	// }

	// resp, err := info.Cli.StarttoGet(context.Background(), &req)
	// if err != nil || resp.Ret == false {
	// 	ret = info.Topic + info.Partition + ":err!=nil or resp.ret==false\n"
	// }
	// if ret == "" {
	// 	return nil
	// } else {
	// 	return errors.New(ret)
	// }
	//根据这个可以得到多个broker
	//这里我不是已经明确了ppartiiton吗，为什么他还会返回多个？
	//partition-1:
	// ├── block-1 → broker-A
	// └── block-2 → broker-B
	resp, err := con.zkBrokerCli.ConGetBro(context.Background(), &api.ConGetBroRequest{
		TopicName:     info.Topic,
		PartitionName: info.Partition,
		Option:        info.Option,
		CliName:       con.Name,
		Index:         info.Offset,
	})
	if err != nil || !resp.Ret {
		return nil, ret, err
	}
	parts = make([]PartKey, resp.Size)
	json.Unmarshal(resp.Parts, &parts)
	//1：ptp
	//3：psb
	if info.Option == 1 || info.Option == 3 {
		ret, err = con.StartGetToBroker(parts, info)
	}
	return parts, ret, err
}

// 连接上各个broker并发送要消息的请求
func (con *Consumer) StartGetToBroker(parts []PartKey, info InfoReq) (ret string, err error) {
	for _, part := range parts {
		if part.Err != "ok" {
			ret += part.Err
			continue
		}
		req := api.InfoGetRequest{
			Consumer_Name:  con.Name,
			Topic_Name:     info.Topic,
			Partition_Name: part.Name,
			Offset:         info.Offset,
			Option:         info.Option,
		}
		bro_cli, ok := con.Brokers[part.BrokerName]
		//说明你想要开始拉取这个个分区所属与的broker你和他还没建立联系
		if !ok {
			bro_cli, err := server_operations.NewClient(con.Name, client.WithHostPorts(part.BrokerHP))
			if err != nil {
				return ret, err
			}
			//option这里需要理解
			//ptp,一旦这样，就持续拉取
			if info.Option == 1 {
				bro_cli.StarttoGet(context.Background(), &req)
				con.Brokers[part.BrokerName] = bro_cli
			}
		}
		//每次都是新的开始
		if info.Option == 3 {
			bro_cli.StarttoGet(context.Background(), &req)
		}
	}
	return ret, nil
}

// 希iangbroker索要信息
func (con *Consumer) Pull(info InfoReq) (int64, int64, []Message, error) {
	resp, err := info.Cli.Pull(context.Background(), &api.PullRequest{
		ConsumerId: con.Name,
		Topic:      info.Topic,
		Key:        info.Partition,
		Offset:     info.Offset,
		Option:     info.Option,
		Size:       info.Size,
	})
	if err != nil {
		return 0, 0, nil, err
	}
	msgs := make([]Message, resp.EndIndex-resp.StartIndex)
	json.Unmarshal(resp.Msgs, &msgs)
	return resp.StartIndex, resp.EndIndex, msgs, nil
}
