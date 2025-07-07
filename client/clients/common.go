package clients

import (
	"net"
)

type PartKey struct {
	Name       string `json:name`
	BrokerName string `json:brokername`
	BrokerHP   string `json:brokerhp`
}

// 本机所有网卡的mac地址进行拼接
func GetIpPort() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "error"
	}
	ipport := ""
	for _, i := range interfaces {
		intermac := i.HardwareAddr.String()
		ipport += intermac
	}
	return ipport
}

type BrokerInfo struct {
	Name      string `json:name`
	Host_Port string `json:host_port`
}
