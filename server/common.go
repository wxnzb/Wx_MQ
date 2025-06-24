package server

import (
	"fmt"
	"net"
	"os"
)

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
func FileOrListExist(filePath string) bool {
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
