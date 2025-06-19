package server

import (
	"encoding/json"
	"os"
	"sync"
)

type File struct {
	rmu       sync.RWMutex
	filePath  string
	node_size int //每次往文件里面写进一批消息，这一批消息前面会有他的topic,partition呀什么的元数据，他时定长的，因此在开始时计算一次就好，为什么他是定常呢？？？
}

func NewFile(filepath string) *File {
	return &File{
		rmu:       sync.RWMutex{},
		filePath:  filepath,
		node_size: 0,
	}
}

// 创建一个新的文件
func (f *File) OpenFile() *os.File {
	f.rmu.Lock()
	defer f.rmu.Unlock()
	file, _ := CreateFile(f.filePath)
	return file
}

// 获取文件的大小
func (f *File) GetSize() int64 {
	f.rmu.RLock()
	defer f.rmu.RUnlock()
	var size int64
	return size
}

// 向文件中写消息
// 这里的node是目录索引,msgs是批量的消息
func (f *File) WriteFile(file *os.File, node KeyData, msgs []Message) bool {
	msgs_json, err := json.Marshal(msgs)
	if err != nil {

	}
	node.Size = len(msgs_json)
	node_json, err := json.Marshal(node)
	if err != nil {

	}
	if f.node_size == 0 {
		f.node_size = len(node_json)
	}
	//写进文件
	f.rmu.Lock()
	file.Write(node_json)
	file.Write(msgs_json)
	f.rmu.Unlock()
	if err != nil {
		return false
	}
	return true
}

// 读取文件的消息
func (f *File) ReadFile(file *os.File) {

}
