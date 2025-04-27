package server

import (
	"Wx_MQ/kitex_gen/api/client_operations"

	cl "github.com/cloudwego/kitex/client"
)

type Server struct {
	topics map[string]Topic
	groups map[string]Group
}

func (s *Server) make() {
	s.topics = make(map[string]Topic)
	s.groups = make(map[string]Group)
	s.groups["defalut"] = Group{}
}
func (s *Server) HanderInfo(ip_port string) error {
	client, err := client_operations.NewClient("client", cl.WithHostPorts(ip_port))
	if err == nil {
		//现在这样写，等于是全部的消费者都加入到了s的一个消费组里面
		s.groups["default"].consumers[ip_port] = &client
		return nil
	}
	return err
}
func (s *Server) HandlerPush() {}
func (s *Server) HanderPull()  {}
