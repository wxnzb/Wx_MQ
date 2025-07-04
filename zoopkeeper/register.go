package register

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"net"

	"github.com/cloudwego/kitex/pkg/registry"
	"github.com/go-zookeeper/zk"
	"github.com/kitex-contrib/registry-zookeeper/entity"
	"github.com/kitex-contrib/registry-zookeeper/utils"
)

type zookeeperRegister struct {
	conn     *zk.Conn
	authOpen bool
	user     string
	passward string
}

func NewzRegister(servers []string, sessionTimeout time.Duration) (registry.Registry, error) {
	conn, _, err := zk.Connect(servers, sessionTimeout)
	if err != nil {
		return nil, err
	}
	return &zookeeperRegister{
		conn: conn,
	}, nil
}
func NewzRegisterWithAuth(servers []string, sessionTimeout time.Duration, user, passward string) (registry.Registry, error) {
	conn, _, err := zk.Connect(servers, sessionTimeout)
	if err != nil {
		return nil, err
	}
	//utils.Scheme="digest" //这里是zookeeper的认证方式
	if err := conn.AddAuth(utils.Scheme, []byte(user+":"+passward)); err != nil {
		return nil, err
	}
	return &zookeeperRegister{
		conn:     conn,
		authOpen: true,
		user:     user,
		passward: passward,
	}, nil
}

//这里实现了registry.Register接口
// func (e noopRegistry) Register(*Info) error {
// 	return nil
// }

// func (e noopRegistry) Deregister(*Info) error {
// 	return nil
// }

// 向zookeeper添加一个临时节点，用来存储服务信息
// 注册服务信息
func (z *zookeeperRegister) Register(info *registry.Info) error {
	path, err := buildPath(info)
	if err != nil {
		return err
	}
	content, err := json.Marshal(&entity.RegistryEntity{
		Weight: info.Weight,
		Tags:   info.Tags,
	})
	if err != nil {
		return err
	}
	return z.createNode(path, content, true)
}

// 删除临时节点
func (z *zookeeperRegister) Deregister(info *registry.Info) error {
	path, err := buildPath(info)
	if err != nil {
		return err
	}
	return z.deleteNode(path)
}

// -------------------------------------------------
// /service-name/ip:port
func buildPath(info *registry.Info) (string, error) {
	var path string
	if !strings.HasPrefix(info.ServiceName, utils.Separator) {
		//utils.Seperator="/"
		path = utils.Separator + info.ServiceName
	}
	host, port, err := net.SplitHostPort(info.Addr.String())
	if err != nil {
		return "", err
	}
	//要是host没有可以获取ipv4的
	if host == "" {
		ipv4, err := utils.GetLocalIPv4Address()
		if err != nil {
			return "", err
		}
		host = ipv4
	}
	//要是port没有就进行不了了
	if port == "" {
		return "", fmt.Errorf("port is empty in address: %s", info.Addr.String())
	}
	path = path + utils.Separator + host + ":" + port
	return path, nil
}
func (z *zookeeperRegister) createNode(path string, content []byte, ephemeral bool) error {
	i := strings.LastIndex(path, utils.Separator)
	if i > 0 {
		err := z.createNode(path[:i], nil, false)
		if err != nil {
			return err
		}
	}
	var flag int32
	if ephemeral {
		flag = zk.FlagEphemeral
	}
	if z.authOpen {
		_, err := z.conn.Create(path, content, flag, zk.DigestACL(zk.PermAll, z.user, z.passward))
		if err != nil {
			return err
		}
	} else {
		_, err := z.conn.Create(path, content, flag, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}
	return nil
}
func (z *zookeeperRegister) deleteNode(path string) error {
	err := z.conn.Delete(path, -1)
	if err != nil {
		return err
	}
	return nil
}
