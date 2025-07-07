package server

import (
	"Wx_MQ/kitex_gen/api/client_operations"
	"fmt"
	"net"
	"os"
)

type PartName struct {
	name string `json:name`
}

// 根据路径创建一个新的文件
func CreateFile(filePath string) (*os.File, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// 这个和client/clients/common.go的GetIpPort函数是一样的
// 本机所有网卡的mac地址进行拼接
func GetIpPort() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		//return "error"
		panic("Poor soul,here is what you got:" + err.Error())
	}
	ipport := ""
	for _, i := range interfaces {
		intermac := i.HardwareAddr.String()
		ipport += intermac
	}
	return ipport
}

// 现在还没有用到
// 判断一个文件是否存在
func FileOrDirExist(filePath string) bool {
	_, err := os.Stat(filePath)
	if err != nil {
		if os.IsExist(err) {
			return false
		}
	}
	return true
}

// 创建目录
func CreateDir(dirPath string) error {
	err := os.Mkdir(dirPath, 0666)
	if err != nil {
		fmt.Println("mkdir", dirPath, "error")
	}
	return err
}

// CheckChangeCon(p.to_consumers, confPartToCons),前面是这个part里面的consumers,后面是config中的这个part里面的consumers，因为+-消费者都是先操作的config,现在就要找出他们量的差异
//
//eg:1 2 3 4 5； 1 2 5 6 7=======reduce:3 4;add : 6 7
func CheckChangeCon(old map[string]*client_operations.Client, new []string) (reduce, add []string) {
	for o, _ := range old {
		ra := false
		for _, n := range new {
			if o == n {
				ra = true //说明都有
				break
			}
		}
		if ra == false {
			reduce = append(reduce, o)
		}
	}
	for _, n := range new {
		ra := false
		for o, _ := range old {
			if n == o {
				ra = true
				break
			}
		}
		if ra == false {
			add = append(add, n)
		}
	}
	return
}
func GetPartNameArry(parts map[string]*Partition) []PartName {
	var varry []PartName
	for p, _ := range parts {
		varry = append(varry, PartName{name: p})
	}
	return varry
}

type Options struct {
	Tag              string
	Name             string
	ZKServerHostPort string
	BrokerHostPort   string
}
type Top_Info struct {
	TopicName  string
	Partitions map[string]Part_Info
	//Pnums    int64
}
type Part_Info struct {
	PartitionName string
	Blocks        map[string]Block_Info
}
type Block_Info struct {
	StartIndex int64
	EndIndex   int64
	FileName   string
}
type Broker_Power struct {
	Name  string `json:"name"`
	Power int64  `json:"power"`
}
type Broker_Assign struct {
	Topics map[string]Top_Info
}
