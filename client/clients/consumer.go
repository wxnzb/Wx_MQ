package clients
//一个消费者客户端可以消费多个broker里面的topic-partition的消息
import (
	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/client_operations"
	"context"
	"fmt"
	"net"

	"Wx_MQ/kitex_gen/api/server_operations"

	server "github.com/cloudwego/kitex/server"
)

type Consumer struct {
	Cli server_operations.Client
    name string
	Topic_Partitions map[string]Info
}
//这里是broker发送给消费者客户端是他们的反应
// 下面这个是启动一个rpc服务器，broker服务器测试消息推送功能或探活功能就是通过和他交流的
//下面这两个会被执行的条件就是broker发送给消费者客户端的消息
//但是我还是感觉很奇怪，为啥他前面是（con *Consumer)??
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

func Start_server(port string) {
	//返回的是*TCPAddr,他实现了Network函数和String函数，也就是实现了net.Addr接口
	addr, _ := net.ResolveTCPAddr("tcp", port)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))
	//第一个参数要实现api.Client_Operations这个接口
	srv := client_operations.NewServer(new(Consumer), opts...)
	//他返回的是一个Server接口，长这样
	// type Server interface {
	// 	RegisterService(svcInfo *serviceinfo.ServiceInfo, handler interface{}, opts ...RegisterOption) error
	// 	GetServiceInfos() map[string]*serviceinfo.ServiceInfo
	// 	Run() error
	// 	Stop() error
	// }
	err := srv.Run()
	if err != nil {
		println(err.Error())
	}
}
//上面的只能被动接受broker发送给你的消息
//要是你想要主动从broker拉取，就看下面
type Info struct{
	topic string
	partition string
	offset int64
	buffer []string
}
func(con *Consumer)StartGet()(err error){
	ret:=""
	for name,info:=range con.Topic_Partitions {
		req:=api.InfoGetRequest{
			CliName:
			TopicName:
			PartatitionName:
			Offset:
		}
		resp,err:=con.Cli.StarttoGet(context.Background(), &req)
		if err!=nil||resp.ret==false{
			ret:=name+":err!=nil or resp.ret==false\n"
		}
}
if ret==""{
	return nil
}else{
	return errors.New(ret)
}
}
