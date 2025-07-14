package server

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
)

type File struct {
	rmu       sync.RWMutex
	filePath  string
	node_size int //每次往文件里面写进一批消息，这一批消息前面会有他的topic,partition呀什么的元数据，他时定长的，因此在开始时计算一次就好，为什么他是定常呢？？？
}
//创建一个文件，要是本来就有，那就打开并以8
func NewFile(filepath string) *File {
	return &File{
		rmu:       sync.RWMutex{},
		filePath:  filepath,
		node_size: 0,
	}
}

// 打开一个已有的文件或者创建一个新的文件
func (f *File) OpenFile() *os.File {
	f.rmu.Lock()
	defer f.rmu.Unlock()
	file, _ := CreateFile(f.filePath)
	return file
}

// [NodeA|PayloadA][NodeB|PayloadB][NodeC|PayloadC]…
// 通过文件的这批消息index获取到这块消息的偏移offset
func (f *File) FindOffset(fd *os.File, index int64) (int64, error) {
	node_data := make([]byte, NODE_SIZE)
	offset := int64(0) //从文件头开始扫描
	var node NodeData
	for {
		//ReadAt 直接按 offset 读取 NODE_SIZE 字节到 node_data
		size, err := fd.ReadAt(node_data, offset)
		//这里为什么size会不等于NODE_SIZE呢

		if size != NODE_SIZE {
			return int64(-1), errors.New("read node size is not NODE_SIZE")
		}
		if err == io.EOF {
			return index, errors.New("blockoffset is out of range")
		}
		json.Unmarshal(node_data, &node)
		//说明现在index还在后面
		if node.End_index < index {
			offset += int64(NODE_SIZE + node.Size) //继续从下一个node开始读
		} else {
			break
		}
	}
	//那他返回的这个offset只能说明这一快消息（几批消息）中有一批他是index，但是还是不能确定他的确切位置，good!!!
	return offset, nil
}

// 获取文件的大小，这个还没用上，就不看了先
func (f *File) GetSize() int64 {
	f.rmu.RLock()
	defer f.rmu.RUnlock()
	var size int64
	return size
}

// 向文件中写消息
// 这里的node是目录索引,msgs是批量的消息
func (f *File) WriteFile(file *os.File, node NodeData, msgs []Message) bool {
	msgs_json, err := json.Marshal(msgs)
	if err != nil {
		DEBUG(dERROR, "%v turn json fail\n", msgs)
	}
	node.Size = len(msgs_json)
	node_json, err := json.Marshal(node)
	if err != nil {
		DEBUG(dERROR, "%v turn json fail\n", node)
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
func (f *File) ReadFile(fd *os.File, blockoffset int64) (NodeData, []Message, error) {
	f.rmu.RLock()
	defer f.rmu.RUnlock()
	node_data := make([]byte, NODE_SIZE)
	size, err := fd.ReadAt(node_data, blockoffset)
	var node NodeData
	var msgs []Message
	//这个好像不需要
	// if err != nil {
	// 	return node,msgs, err
	// }
	if size != NODE_SIZE {
		return node, msgs, errors.New("read node size is not NODE_SIZE")
	}
	if err == io.EOF {
		return node, msgs, errors.New("blockoffset is out of range")
	}
	//var node NodeData
	json.Unmarshal(node_data, &node)

	offset := blockoffset + int64(NODE_SIZE)
	msgs_data := make([]byte, node.Size)
	size, err = fd.ReadAt(msgs_data, offset)
	//这个好像也不需要
	// if err != nil {

	// }
	if size != node.Size {
		return node, msgs, errors.New("read node size is not NODE_SIZE")
	}
	if err == io.EOF {
		return node, msgs, errors.New("blockoffset is out of range")
	}
	// var msgs []Message
	json.Unmarshal(msgs_data, &msgs)
	return node, msgs, nil

}

// 这个还没有实现
func (f *File) GetIndex(file *os.File) int64 {
	return 0
}

// 修改文件名
func (f *File) UpFileName(newname string) {
	f.rmu.Lock()
	defer f.rmu.Unlock()
}
