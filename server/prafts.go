package server

import (
	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/raft_operations"
	"context"
	"net"
	"sync"
	"time"

	"Wx_MQ/raft"

	"encoding/json"

	"github.com/cloudwego/kitex/server"
)

type parts_raft struct {
	rmu      sync.RWMutex
	srv_raft server.Server
	parts    map[string]*raft.Raft
	//---------------
	//当前raft节点的编号
	me int
	//Client Done Map，用于记录每个produce在每个 topic+partition 上已经完成提交的最大 cmdindex
	CDM map[string]map[string]int64
	CSM map[string]map[string]int64
	// applyindex["topicA-part1"] = 250
	// topicA-part1，状态机已经处理到 Raft 日志的第 250 条日志
	applyindex map[string]int
	Add        chan COMD
	appench    chan Info
	applyCh    chan raft.ApplyMsg
}
type COMD struct {
	index int
	num   int
}

func NewPartRaft() *parts_raft {
	return &parts_raft{
		rmu:   sync.RWMutex{},
		parts: make(map[string]*raft.Raft),
	}
}

// 提交写请求：
// 上层通过 Add 通道发送 COMD。
// parts_raft 调用对应的 Raft 实例，把命令写入日志并复制。
// 日志达成一致：
// Raft 把 ApplyMsg 发送到 applyCh。
// parts_raft 读取 applyCh，应用状态更新。
// 回调上层：
// 通过 appench 通知上层（Broker），完成最终逻辑。
// 这个name根本就没有用到为什么要传进去
func (praft *parts_raft) make(name string, hostport string, appench chan Info) error {
	//当 Raft 应用日志（ApplyMsg）时，parts_raft 会通过这个 channel 把信息（比如更新状态、分区信息）通知上层broker
	praft.appench = appench
	//Raft 共识成功后会把 ApplyMsg 发送到这个 channel，表示某条日志可以被状态机应用
	praft.applyCh = make(chan raft.ApplyMsg)
	// 当 Broker 要执行某个命令，会把命令包装成 COMD 结构，发送到这个 channel
	praft.Add = make(chan COMD)

	praft.CDM = make(map[string]map[string]int64)
	praft.CSM = make(map[string]map[string]int64)
	//记录每个分区的最新应用日志索引,如果 Broker 重启，可以根据 apply index 恢复状态。
	praft.applyindex = make(map[string]int)
	//保存每个 分区 对应的 Raft 实例
	praft.parts = make(map[string]*raft.Raft)

	addr, _ := net.ResolveTCPAddr("tcp", hostport)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))
	//这里创建的praft.srv_raft和上面praft.parts = make(map[string]*raft.Raft)这个的关系
	//第一个是1个rpc服务，负责：接收来自其他 Broker 的 Raft RPC；发这些请求给对应的分区 Raft 实例
	praft.srv_raft = raft_operations.NewServer(praft, opts...)
	err := praft.srv_raft.Run()
	if err != nil {
		return err
	}
	return nil
}

// 这些地层调用的都是parts    map[string]*raft.Raft这个对应的raft结构体
func (praft *parts_raft) RequestVote(ctx context.Context, args_ *api.RequestVoteArgs_) (r *api.ResponseVoteReply, err error) {
	str := args_.TopicName + args_.PartitionName
	resp := praft.parts[str].RequestVoteHandle(&raft.RequestVoteArgs{
		Term:         args_.Term,
		CandidateId:  args_.CandidateId,
		LastLogIndex: args_.LastLogIndex,
		LastLogTerm:  args_.LastLogTerm,
	})
	return &api.ResponseVoteReply{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (praft *parts_raft) AppendEntries(ctx context.Context, args_ *api.AppendEntriesArgs_) (r *api.AppendEntriesReply, err error) {
	str := args_.TopicName + args_.PartitionName
	var logs []raft.LogNode
	json.Unmarshal(args_.Entries, &logs)
	resp := praft.parts[str].AppendEntriesHandle(&raft.AppendEntriesArgs{
		Term:         args_.Term,
		LeaderId:     args_.LeaderId,
		PrevLogIndex: args_.PrevLogIndex,
		PrevLogTerm:  args_.PrevLogTerm,
		Entries:      logs,
		LeaderCommit: args_.LeaderCommit,
	})
	return &api.AppendEntriesReply{
		Term:           resp.Term,
		Success:        resp.Success,
		TermFirstIndex: resp.TermFirstIndex,
		LogTerm:        resp.LogTerm,
	}, nil
}

func (praft *parts_raft) SnapShot(ctx context.Context, args_ *api.SnapShotArgs_) (r *api.SnapShotReply, err error) {
	str := args_.TopicName + args_.PartitionName
	resp := praft.parts[str].SnapShotHandle(&raft.SnapShotArgs{
		Term:                 args_.Term,
		LeaderId:             args_.LeaderId,
		LastIncludedLogIndex: args_.LastIncludedLogIndex,
		LastIncludedLogTerm:  args_.LastIncludedLogTerm,
		Log:                  args_.Log,
		SnapShot:             args_.Log,
	})
	return &api.SnapShotReply{
		Term: resp.Term,
	}, nil
}

const (
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

type Op struct {
	Ser_index int64
	Cli_index string
	Cmd_index int64
	Topic     string
	Partition string
	Message   string
	Tpart     string
	Size      int8
	Operation string
}

// Raft 分区组 praft接收到来自客户端的 Append 请求，尝试将 in.message 写入分区 in.topic_name/in.part_name，返回结果字符串和错误
func (praft *parts_raft) Append(in Info) (ret string, err error) {
	str := in.topic + in.partition
	praft.rmu.Lock()
	//找到broker下的这个特定的raft实例并判断这个实例是否是leader
	_, isLeader := praft.parts[str].GetState()
	if !isLeader {
		DEBUG(dLog, "S%d this is not leader\n", praft.me)
		return ErrWrongLeader, nil
	}
	//检查快照是否已恢复,没恢复说明还没准备好,applyindex = 状态机应用的最新日志索引
	if praft.applyindex[str] == 0 {
		DEBUG(dLog, "S%d the snap noot applied applyindex is %v\n", praft.me, praft.applyindex[str])
		praft.rmu.Unlock()
		time.Sleep(200 * time.Millisecond)
		return ErrTimeOut, nil
	}
	//先检查有这个结构吗？
	_, ok := praft.CDM[str]
	if !ok {
		praft.CDM[str] = make(map[string]int64)
	}
	_, ok = praft.CSM[str]
	if !ok {
		praft.CSM[str] = make(map[string]int64)
	}
	//幂等性检查（CDM）
	in1, okk1 := praft.CDM[str][in.producer]
	//当前请求的 cmdindex == 每个生产者的最后一次提交的命令索引，说明这条命令已经成功应用过
	if okk1 && in1 == in.cmdindex {
		praft.rmu.Unlock()
		return OK, nil
	} else if !okk1 {
		praft.CDM[str][in.producer] = 0
	}
	praft.rmu.Unlock()
	//封装一个 Op（操作命令），用于 Raft 日志复制
	O := raft.Op{
		Ser_index: int64(praft.me),
		Cli_name:  in.producer,
		Cmd_index: in.cmdindex,
		Topic:     in.topic,
		Part:      in.partition,
		Msg:       in.message,
		Tpart:     str,
		Size:      in.size,
		Operate:   "Append",
	}
	//如果该请求（来自producer）还没有提交到 Raft，就调用 Start() 提交；如果已经提交过，就不重复提交
	praft.rmu.Lock()
	DEBUG(dLog, "S%d lock 285\n", praft.me)
	in2, okk2 := praft.CSM[str][in.producer]
	if !okk2 {
		praft.CSM[str][in.producer] = 0
	}
	//这里要判断leader
	//即使已经提交（cmdindex 和 CSM 相等），但还要确保你是 leader，否则你不能承认自己已经处理了这条命令
	var index int
	praft.rmu.Unlock()
	//这里等于为什么不像上面一样返回：记录生产者最后一次提交的命令（但未必应用），上面那个不仅提交而且已经应用(apply)
	if in2 == in.cmdindex {
		_, isLeader = praft.parts[str].GetState()
	} else {
		//Start() 成功 → 写 CSM → 日志 apply → 写 CDM
		index, _, isLeader = praft.parts[str].Start(O, false, 0)
	}
	if !isLeader {
		return ErrWrongLeader, nil
	} else {
		praft.rmu.Lock()
		DEBUG(dLog, "S%D lock 312\n", praft.me)
		lastindex, ok := praft.CSM[str][in.producer]
		if !ok {
			praft.CSM[str][in.producer] = 0
		}
		//如果 Start() 成功，更新 CSM
		praft.CSM[str][in.producer] = in.cmdindex
		praft.rmu.Unlock()
		//等待 apply 成功或超时
		for {
			select {
			case out := <-praft.Add:
				if index == out.index {
					return OK, nil
				} else {
					DEBUG(dLog, "S%d index!=out.index pytappend %d != %d\n", praft.me, index, out.index)
				}
			case <-time.After(TOUT * time.Microsecond):
				_, isLeader = praft.parts[str].GetState()
				ret := ErrTimeOut
				praft.rmu.Lock()
				DEBUG(dLog, "S%d lock 332\n", praft.me)
				DEBUG(dLeader, "S%d time out\n", praft.me)
				if !isLeader {
					ret = ErrWrongLeader
					praft.CSM[str][in.producer] = lastindex
				}
				praft.rmu.Unlock()
				return ret, nil
			}
		}
	}
}

// 添加一个需要raft同步的partition
func (praft *parts_raft) AddPart_Raft(peers []*raft_operations.Client, me int, topic, part string, aplych chan Info) {
	str := topic + part
	praft.rmu.Lock()
	_, ok := praft.parts[str]
	if !ok {
		per := &raft.Persister{}
		part_raft := raft.Make(peers, me, per, aplych, topic, part)
		praft.parts[str] = part_raft
	}
	praft.rmu.Unlock()
}
