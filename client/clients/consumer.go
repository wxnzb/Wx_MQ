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

	"errors"

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
	srv   server.Server
	//下面这些的作用是什么
	port        string ///这个port是给broker注册的
	zkBrokerCli zkserver_operations.Client
	Brokers     map[string]*server_operations.Client
}

func NewConsumer(zkBrokerIpport, name, port string) *Consumer {
	c := &Consumer{
		rmu:     sync.RWMutex{},
		state:   "alive",
		Name:    name,
		port:    port,
		Brokers: make(map[string]*server_operations.Client),
	}
	c.zkBrokerCli, _ = zkserver_operations.NewClient(name, client.WithHostPorts(zkBrokerIpport))
	return c
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

func (c *Consumer) SubScription(topic_name, partition_name string, option int8) (clis []*server_operations.Client, err error) {
	resp, err := c.zkBrokerCli.ConGetBro(context.Background(), &api.ConGetBroRequest{
		TopicName:     topic_name,
		PartitionName: partition_name,
		Option:        option,
	})
	if err != nil || resp.Ret == false {
		return
	}
	//一个消费者订阅的topic_part可以属于多个brokers
	//现在你还不明白为什么返回的[]byte这个resp.Bros为什么可以直接转化为BrokerInfo这个结构，然后就是返回的这个Size具体是什么？？？
	brokers := make([]BrokerInfo, resp.Size)
	json.Unmarshal(resp.Bros, &brokers)
	for _, bro := range brokers {
		cli, _ := server_operations.NewClient(c.Name, client.WithHostPorts(bro.Host_Port))
		c.Brokers[bro.Host_Port] = &cli
		clis = append(clis, &cli)
		c.RegisterSelf(c.port, &cli) //完成反向注册，这样broker就可以主动发送pingpong和pub了
		resp, _ := cli.Sub(context.Background(), &api.SubRequest{
			Topic:  topic_name,
			Key:    partition_name,
			Option: option,
		})
		if resp.Ret == false {
			return clis, err
		}
	}
	return clis, nil
}

type InfoReq struct {
	Topic     string
	Partition string
	Offset    int64
	Option    int8
	Bufs      map[int64]*api.PubRequest
	Cli       server_operations.Client
}

func NewInfoReq(topic, partition string, offset int64) *InfoReq {
	return &InfoReq{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Bufs:      make(map[int64]*api.PubRequest),
	}
}

// 这个消息发送后，broker就会开始向这个消费者发送消息(pub)
func (con *Consumer) StartGet(info InfoReq) (err error) {
	ret := ""
	req := api.InfoGetRequest{
		Cli_Name:       con.Name,
		Topic_Name:     info.Topic,
		Partition_Name: info.Partition,
		Offset:         info.Offset,
		Option:         info.Option,
	}
	resp, err := info.Cli.StarttoGet(context.Background(), &req)
	if err != nil || resp.Ret == false {
		ret = info.Topic + info.Partition + ":err!=nil or resp.ret==false\n"
	}
	if ret == "" {
		return nil
	} else {
		return errors.New(ret)
	}
}
