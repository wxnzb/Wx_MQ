package server

import (
	"bytes"
	"encoding/binary"
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

// 先检查该磁盘是否存在该文件，要是不存在在进行创建
func NewFile(filepath string) (file *File, fd *os.File, Err string, err error) {
	if !FileOrDirExist(filepath) {
		fd, err = CreateFile(filepath)
		if err != nil {
			Err = "CreateFileFail"
			return nil, nil, Err, err
		}
	} else {
		fd, err = os.OpenFile(filepath, os.O_RDWR, 0666)
		if err != nil {
			Err = "OpenFileFail"
			return nil, nil, Err, err
		}
	}
	file = &File{
		rmu:       sync.RWMutex{},
		filePath:  filepath,
		node_size: NODE_SIZE,
	}
	return file, fd, "ok", err
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
		f.rmu.RLock()
		//ReadAt 直接按 offset 读取 NODE_SIZE 字节到 node_data
		size, err := fd.ReadAt(node_data, offset)
		f.rmu.RUnlock()
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

// 遍历一个文件里按记录写入的若干 “节点头 + 数据块” 记录，找到并返回最后一个记录的 End_index（也就是该文件当前的最大索引）
func (f *File) GetIndex(file *os.File) int64 {
	data_node := make([]byte, NODE_SIZE)
	var offset int64
	offset = 0
	var index int64
	index = -1
	var node NodeData
	for {
		_, err := file.ReadAt(data_node, offset)
		//读到文件末尾了
		if err == io.EOF {
			if index == 0 {
				json.Unmarshal(data_node, &node)
				index = node.End_index
			} else {
				index = 0
			}
			return index
		} else {
			index = 0
		}
		buf := &bytes.Buffer{}
		binary.Write(buf, binary.BigEndian, data_node)
		binary.Read(buf, binary.BigEndian, &node)
		offset = offset + NODE_SIZE + int64(node.Size)
	}
}

// 修改本地文件文件名
func (f *File) UpFileName(path, newname string) error {
	oldFilePath := f.filePath
	newFilePath := path + "/" + newname
	f.rmu.Lock()
	f.filePath = newFilePath
	defer f.rmu.Unlock()
	return MovName(oldFilePath, newFilePath)
}
func (f *File) GetFirstIndex(file *os.File) int64 {
	f.rmu.RLock()
	defer f.rmu.RUnlock()
	data_node := make([]byte, NODE_SIZE)
	_, err := file.ReadAt(data_node, 0)
	if err == io.EOF {
		return 0
	}
	buf := &bytes.Buffer{}
	var node NodeData
	binary.Write(buf, binary.BigEndian, data_node)
	binary.Read(buf, binary.BigEndian, &node)
	return node.Start_index
}
