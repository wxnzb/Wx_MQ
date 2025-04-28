package main

//这个就是创建一个broker服务器
import (
	"net"
	//这两个之间必须要给一个重新起一个名字，不然都是server.，那就会起冲突
	Server "Wx_MQ/server"

	"fmt"

	"github.com/cloudwego/kitex/server"
)

func main() {
	addr, _ := net.ResolveTCPAddr("tcp", ":8888")
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))
	rpcServer := new(Server.RPCServer)
	err := rpcServer.Start(opts)
	if err != nil {
		fmt.Println(err.Error())
	}
}
