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

	server "github.com/cloudwego/kitex/server"
)

type Consumer struct {
	rmu sync.RWMutex
	//这里是大写是因为其他地方要用到
	Cli   server_operations.Client //到broker的RPC客户端句柄
	Name  string
	state string
	srv   server.Server
}

func NewConsumer() *Consumer {
	return &Consumer{
		rmu:   sync.RWMutex{},
		state: "alive",
	}
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
func (con *Consumer) Start_server(port string) {
	//返回的是*TCPAddr,他实现了Network函数和String函数，也就是实现了net.Addr接口
	addr, _ := net.ResolveTCPAddr("tcp", port)
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
func (con *Consumer) RegisterSelf(port string) error {
	resp, err := con.Cli.Info(context.Background(), &api.InfoRequest{
		IpPort: port,
	})
	if err != nil {
		fmt.Print(resp)
	}
	return err
}
func (c *Consumer) SubScription(sub api.SubRequest) (ret []PartName, err error) {
	resp, err := c.Cli.Sub(context.Background(), &sub)
	if err != nil || resp.Ret == false {
		return ret, err
	}
	parts := make([]PartName, resp.Size)
	json.Unmarshal(resp.Parts, &parts)

	return parts, nil
}

// 上面的只能被动接受broker发送给你的消息
// 要是你想要主动从broker拉取，就看下面
// 这个还没有写好
type Info struct {
	topic     string
	partition string
	offset    int64
	//这两个干啥
	option int8
	bufs   map[int64]*api.PubRequest
}

func (con *Consumer) StartGet(info Info) (err error) {
	ret := ""
	req := api.InfoGetRequest{
		Cli_Name:       con.Name,
		Topic_Name:     info.topic,
		Partition_Name: info.partition,
		Offset:         info.offset,
		Option:         info.option,
	}
	resp, err := con.Cli.StarttoGet(context.Background(), &req)
	if err != nil || resp.Ret == false {
		ret = info.topic + info.partition + ":err!=nil or resp.ret==false\n"
	}
	if ret == "" {
		return nil
	} else {
		return errors.New(ret)
	}
}
