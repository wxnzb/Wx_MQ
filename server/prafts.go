package server

import (
	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/raft_operations"
	"Wx_MQ/logger"
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"Wx_MQ/raft"

	"encoding/json"

	"github.com/cloudwego/kitex/server"
)

const (
	TIMEOUT        = 1000 * 1000
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
	ErrWrongNum    = "ErrWrongNum"
)

type SnapShot struct {
	Tpart string

	Csm map[string]int64
	Cdm map[string]int64

	Apliedindex int
}
type parts_raft struct {
	rmu      sync.RWMutex
	srv_raft server.Server
	parts    map[string]*raft.Raft
	//---------------
	//当前raft节点的编号
	me      int
	appench chan Info
	applyCh chan raft.ApplyMsg
	//Client Done Map，用于记录每个produce在每个 topic+partition 上已经完成提交的最大 cmdindex
	CDM map[string]map[string]int64
	CSM map[string]map[string]int64
	// applyindex["topicA-part1"] = 250
	// topicA-part1，状态机已经处理到 Raft 日志的第 250 条日志
	applyindex map[string]int
	Add        chan COMD

	maxraftstate int          // snapshot if log grows this big
	dead         int32        // set by Kill()
	ChanComd     map[int]COMD //管道getkvs的消息队列
	// rpcindex   int
	Now_Num int
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

//  RequestVote(ctx context.Context, args_ *RequestVoteArgs_) (r *ResponseVoteReply, err error)

// 	AppendEntries(ctx context.Context, args_ *AppendEntriesArgs_) (r *AppendEntriesReply, err error)

// 	SnapShot(ctx context.Context, args_ *SnapShotArgs_) (r *SnapShotReply, err error)

//	Pingpong(ctx context.Context, req *PingPongArgs_) (r *PingPongReply, err error)
//
// 提交写请求：
// 上层通过 Add 通道发送 COMD。
// parts_raft 调用对应的 Raft 实例，把命令写入日志并复制。
// 日志达成一致：
// Raft 把 ApplyMsg 发送到 applyCh。
// parts_raft 读取 applyCh，应用状态更新。
// 回调上层：
// 通过 appench 通知上层（Broker），完成最终逻辑。
// 这个name根本就没有用到为什么要传进去
func (praft *parts_raft) make(name string, opts []server.Option, appench chan Info, me int) error {
	//当 Raft 应用日志（ApplyMsg）时，parts_raft 会通过这个 channel 把信息（比如更新状态、分区信息）通知上层broker
	praft.appench = appench
	praft.me = me
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

	srv_raft := raft_operations.NewServer(praft, opts...)
	praft.srv_raft = srv_raft

	err := praft.srv_raft.Run()
	if err != nil {
		logger.DEBUG_RAFT(logger.DError, "the raft run fail %v\n", err.Error())
	}
	return nil
}

// 这些地层调用的都是parts    map[string]*raft.Raft这个对应的raft结构体
func (praft *parts_raft) RequestVote(ctx context.Context, args_ *api.RequestVoteArgs_) (r *api.ResponseVoteReply, err error) {
	str := args_.TopicName + args_.PartitionName
	resp := praft.parts[str].RequestVoteHandle(&raft.RequestVoteArgs{
		Term:         int(args_.Term),
		CandidateId:  int(args_.CandidateId),
		LastLogIndex: int(args_.LastLogIndex),
		LastLogTerm:  int(args_.LastLogTerm),
	})
	return &api.ResponseVoteReply{
		Term:        int8(resp.Term),
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (praft *parts_raft) AppendEntries(ctx context.Context, args_ *api.AppendEntriesArgs_) (r *api.AppendEntriesReply, err error) {
	str := args_.TopicName + args_.PartitionName
	var logs []raft.LogNode
	json.Unmarshal(args_.Entries, &logs)
	resp := praft.parts[str].AppendEntriesHandle(&raft.AppendEntriesArgs{
		Term:         int(args_.Term),
		LeaderId:     int(args_.LeaderId),
		PrevLogIndex: int(args_.PrevLogIndex),
		PrevLogTerm:  int(args_.PrevLogTerm),
		Entries:      logs,
		LeaderCommit: int(args_.LeaderCommit),
	})
	return &api.AppendEntriesReply{
		Term:           int8(resp.Term),
		Success:        resp.Success,
		TermFirstIndex: int8(resp.TermFirstIndex),
		LogTerm:        int8(resp.LogTerm),
	}, nil
}

func (praft *parts_raft) SnapShot(ctx context.Context, args_ *api.SnapShotArgs_) (r *api.SnapShotReply, err error) {
	str := args_.TopicName + args_.PartitionName
	resp := praft.parts[str].SnapShotHandle(&raft.SnapShotArgs{
		Term:                 int(args_.Term),
		LeaderId:             int(args_.LeaderId),
		LastIncludedLogIndex: int(args_.LastIncludedLogIndex),
		LastIncludedLogTerm:  int(args_.LastIncludedLogTerm),
		SnapShot:             args_.Log,
	})
	return &api.SnapShotReply{
		Term: int8(resp.Term),
	}, nil
}
func (p *parts_raft) Pingpong(ctx context.Context, rep *api.PingPongArgs_) (r *api.PingPongReply, err error) {

	return &api.PingPongReply{
		Pong: true,
	}, nil
}

// Raft 分区组 praft接收到来自客户端的 Append 请求，尝试将 in.message 写入分区 in.topic_name/in.part_name，返回结果字符串和错误
func (praft *parts_raft) Append(in Info) (ret string, err error) {
	str := in.topic + in.partition
	logger.DEBUG_RAFT(logger.DLeader, "S%d <-- C%v putappend message(%v) topic_partition(%v)\n", praft.me, in.producer, in.cmdindex, str)
	praft.rmu.Lock()

	//找到broker下的这个特定的raft实例并判断这个实例是否是leader
	_, isLeader := praft.parts[str].GetState()
	if !isLeader {
		logger.DEBUG_RAFT(logger.DLog, "raft %d is not the leader", praft.me)
		praft.rmu.Unlock()
		time.Sleep(200 * time.Millisecond)
		return ErrWrongLeader, nil
	}
	//检查快照是否已恢复,没恢复说明还没准备好,applyindex = 状态机应用的最新日志索引
	if praft.applyindex[str] == 0 {
		logger.DEBUG_RAFT(logger.DLog, "%d the snap not applied applyindex is %v\n", praft.me, praft.applyindex[str])
		praft.rmu.Unlock()
		time.Sleep(200 * time.Millisecond)
		return ErrTimeOut, nil
	}
	//先检查是否进行初始化
	_, ok := praft.CDM[str]
	if !ok {
		logger.DEBUG_RAFT(logger.DLog, "%d make CDM Tpart(%v)", praft.me, str)
		praft.CDM[str] = make(map[string]int64)
	}
	_, ok = praft.CSM[str]
	if !ok {
		logger.DEBUG_RAFT(logger.DLog, "%d make CSM Tpart(%v)", praft.me, str)
		praft.CSM[str] = make(map[string]int64)
	}
	//幂等性检查（CDM）
	in1, okk1 := praft.CDM[str][in.producer]
	//当前请求的 cmdindex == 每个生产者的最后一次提交的命令索引，说明这条命令已经成功应用过
	if okk1 && in1 == in.cmdindex {
		logger.DEBUG_RAFT(logger.DInfo, "%d p.CDM[%v][%v](%v) in.cmdindex(%v)\n", praft.me, str, in.producer, praft.CDM[str][in.producer], in.cmdindex)
		praft.rmu.Unlock()
		return OK, nil
	} else if !okk1 {
		logger.DEBUG_RAFT(logger.DLog, "%d add CDM[%v][%v](%v)\n", praft.me, str, in.producer, 0)
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
	logger.DEBUG_RAFT(logger.DLog, "%d lock 285\n", praft.me)
	in2, okk2 := praft.CSM[str][in.producer]
	if !okk2 {
		praft.CSM[str][in.producer] = 0
	}
	//这里要判断leader
	//即使已经提交（cmdindex 和 CSM 相等），但还要确保你是 leader，否则你不能承认自己已经处理了这条命令
	praft.rmu.Unlock()
	logger.DEBUG_RAFT(logger.DInfo, "%d p.CSM[%v][%v](%v) in.cmdIndex(%v)\n", praft.me, str, in.producer, praft.CSM[str][in.producer], in.cmdindex)
	var index int
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
					logger.DEBUG_RAFT(logger.DLog, "%d cmd index %d != out.index %d\n", praft.me, index, out.index)
				}
			case <-time.After(TOUT * time.Microsecond):
				_, isLeader = praft.parts[str].GetState()
				ret := ErrTimeOut
				praft.rmu.Lock()
				logger.DEBUG_RAFT(logger.DLog, "%d lock 332\n", praft.me)
				logger.DEBUG_RAFT(logger.DLeader, "%d time out\n", praft.me)
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
		part_raft := raft.Make(peers, me, per, praft.applyCh, topic, part)
		praft.parts[str] = part_raft
	}
	praft.rmu.Unlock()
}
func (praft *parts_raft) DeletePart_raft(topicname, partitionname string) error {
	str := topicname + partitionname
	praft.rmu.Lock()
	defer praft.rmu.Unlock()
	raft, ok := praft.parts[str]
	if !ok {
		logger.DEBUG_RAFT(logger.DError, "this tpoic-partition(%v) is not in this broker\n", str)
		return errors.New("this tpoic-partition is not in this broker")
	} else {
		raft.Kill()
		delete(praft.parts, str)
	}
	return nil
}

// 设置partitoin中谁来做leader
func (p *parts_raft) SetLeader() {

}
func (p *parts_raft) StartServer() {

	logger.DEBUG_RAFT(logger.DSnap, "S%d parts_raft start\n", p.me)

	go func() {

		for {
			if !p.killed() {
				select {
				case m := <-p.applyCh:

					if m.BeLeader {
						str := m.TopicName + m.PartName
						logger.DEBUG_RAFT(logger.DLog, "S%d Broker tPart(%v) become leaderaply from %v to %v\n", p.me, str, p.applyindex[str], m.CommandIndex)
						p.applyindex[str] = m.CommandIndex

						if m.Leader == p.me {
							p.appench <- Info{
								producer:  "Leader",
								topic:     m.TopicName,
								partition: m.PartName,
							}
						}
					} else if m.CommandValid && !m.BeLeader {
						start := time.Now()

						logger.DEBUG_RAFT(logger.DLog, "S%d try lock 847\n", p.me)
						p.rmu.Lock()
						logger.DEBUG_RAFT(logger.DLog, "S%d success lock 847\n", p.me)
						ti := time.Since(start).Milliseconds()
						logger.DEBUG_RAFT(logger.DLog2, "S%d AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA%d\n", p.me, ti)

						O := m.Command

						_, ok := p.CDM[O.Tpart]
						if !ok {
							logger.DEBUG_RAFT(logger.DLog, "S%d make CDM Tpart(%v)\n", p.me, O.Tpart)
							p.CDM[O.Tpart] = make(map[string]int64)
							// if O.Cli_name != "TIMEOUT" {
							// 	p.CDM[]
							// }
						}
						_, ok = p.CSM[O.Tpart]
						if !ok {
							logger.DEBUG_RAFT(logger.DLog, "S%d make CSM Tpart(%v)\n", p.me, O.Tpart)
							p.CSM[O.Tpart] = make(map[string]int64)
						}

						logger.DEBUG_RAFT(logger.DLog, "S%d TTT CommandValid(%v) applyindex[%v](%v) CommandIndex(%v) CDM[C%v][%v](%v) O.Cmd_index(%v) from(%v)\n", p.me, m.CommandValid, O.Tpart, p.applyindex[O.Tpart], m.CommandIndex, O.Tpart, O.Cli_name, p.CDM[O.Tpart][O.Cli_name], O.Cmd_index, O.Ser_index)

						if p.applyindex[O.Tpart]+1 == m.CommandIndex {

							if O.Cli_name == "TIMEOUT" {
								logger.DEBUG_RAFT(logger.DLog, "S%d for TIMEOUT update applyindex %v to %v\n", p.me, p.applyindex[O.Tpart], m.CommandIndex)
								p.applyindex[O.Tpart] = m.CommandIndex
							} else if p.CDM[O.Tpart][O.Cli_name] < O.Cmd_index {
								logger.DEBUG_RAFT(logger.DLeader, "S%d get message update CDM[%v][%v] from %v to %v update applyindex %v to %v\n", p.me, O.Tpart, O.Cli_name, p.CDM[O.Tpart][O.Cli_name], O.Cmd_index, p.applyindex[O.Tpart], m.CommandIndex)
								p.applyindex[O.Tpart] = m.CommandIndex

								p.CDM[O.Tpart][O.Cli_name] = O.Cmd_index
								if O.Operate == "Append" {

									p.appench <- Info{
										producer:  O.Cli_name,
										message:   O.Msg,
										topic:     O.Topic,
										partition: O.Part,
										size:      O.Size,
									}

									select {
									case p.Add <- COMD{index: m.CommandIndex}:
										// logger.DEBUG_RAFT(logger.DLog, "S%d write putAdd in(%v)\n", kv.me, kv.gid, m.CommandIndex)
									default:
										// logger.DEBUG_RAFT(logger.DLog, "S%d can not write putAdd in(%v)\n", kv.me, kv.gid, m.CommandIndex)
									}

								}
							} else if p.CDM[O.Tpart][O.Cli_name] == O.Cmd_index {
								logger.DEBUG_RAFT(logger.DLog2, "S%d this cmd had done, the log had two update applyindex %v to %v\n", p.me, p.applyindex[O.Tpart], m.CommandIndex)
								p.applyindex[O.Tpart] = m.CommandIndex
							} else {
								logger.DEBUG_RAFT(logger.DLog2, "S%d the topic_partition(%v) producer(%v) OIndex(%v) < CDM(%v)\n", p.me, O.Tpart, O.Cli_name, O.Cmd_index, p.CDM[O.Tpart][O.Cli_name])
								p.applyindex[O.Tpart] = m.CommandIndex
							}

						} else if p.applyindex[O.Tpart]+1 < m.CommandIndex {
							logger.DEBUG_RAFT(logger.DWarn, "S%d the applyindex + 1 (%v) < commandindex(%v)\n", p.me, p.applyindex[O.Tpart], m.CommandIndex)
							// kv.applyindex = m.CommandIndex
						}

						if p.maxraftstate > 0 {
							p.CheckSnap()
						}

						p.rmu.Unlock()
						logger.DEBUG_RAFT(logger.DLog, "S%d Unlock 1369\n", p.me)

						// if maxraftstate > 0 {
						// 	go kv.CheckSnap()
						// }

					} else { //read snapshot
						r := bytes.NewBuffer(m.Snapshot)
						d := raft.NewDecoder(r)
						logger.DEBUG_RAFT(logger.DSnap, "S%d the snapshot applied\n", p.me)
						var S SnapShot
						p.rmu.Lock()
						logger.DEBUG_RAFT(logger.DLog, "S%d lock 1029\n", p.me)
						if d.Decode(&S) != nil {
							p.rmu.Unlock()
							logger.DEBUG_RAFT(logger.DLog, "S%d Unlock 1384\n", p.me)
							logger.DEBUG_RAFT(logger.DSnap, "S%d labgob fail\n", p.me)
						} else {
							p.CDM[S.Tpart] = S.Cdm
							p.CSM[S.Tpart] = S.Csm
							// kv.config = S.Config
							// kv.rpcindex = S.Rpcindex
							// kv.check = false
							logger.DEBUG_RAFT(logger.DSnap, "S%d recover by SnapShot update applyindex(%v) to %v\n", p.me, p.applyindex[S.Tpart], S.Apliedindex)
							p.applyindex[S.Tpart] = S.Apliedindex
							p.rmu.Unlock()
							logger.DEBUG_RAFT(logger.DLog, "S%d Unlock 1397\n", p.me)
						}

					}

				case <-time.After(TIMEOUT * time.Microsecond):
					O := raft.Op{
						Ser_index: int64(p.me),
						Cli_name:  "TIMEOUT",
						Cmd_index: -1,
						Operate:   "TIMEOUT",
					}
					logger.DEBUG_RAFT(logger.DLog, "S%d have log time applied\n", p.me)
					p.rmu.RLock()
					for str, raft := range p.parts {
						O.Tpart = str
						raft.Start(O, false, 0)
					}
					p.rmu.RUnlock()
				}
			}
		}

	}()
}
func (p *parts_raft) SendSnapShot(str string) {
	w := new(bytes.Buffer)
	e := raft.NewEncoder(w)
	S := SnapShot{
		Csm:   p.CSM[str],
		Cdm:   p.CDM[str],
		Tpart: str,
		// Rpcindex:    kv.rpcindex,
		Apliedindex: p.applyindex[str],
	}
	e.Encode(S)
	logger.DEBUG_RAFT(logger.DSnap, "S%d the size need to snap\n", p.me)
	data := w.Bytes()
	go p.parts[str].Snapshot(S.Apliedindex, data)

}
func (p *parts_raft) CheckPartState(TopicName, PartName string) bool {
	str := TopicName + PartName
	p.rmu.Lock()
	defer p.rmu.Unlock()

	_, ok := p.parts[str]
	return ok
}
func (p *parts_raft) CheckSnap() {
	// kv.mu.Lock()

	for str, raft := range p.parts {
		X, num := raft.RaftSize()
		logger.DEBUG_RAFT(logger.DSnap, "S%d the size is (%v) applidindex(%v) X(%v)\n", p.me, num, p.applyindex[str], X)
		if num >= int(float64(p.maxraftstate)) {
			if p.applyindex[str] == 0 || p.applyindex[str] <= X {
				// kv.mu.Unlock()
				return
			}
			p.SendSnapShot(str)
		}
	}
	// kv.mu.Unlock()
}
func (p *parts_raft) Kill(str string) {
	atomic.StoreInt32(&p.dead, 1)
	logger.DEBUG_RAFT(logger.DLog, "S%d kill\n", p.me)
}

func (p *parts_raft) killed() bool {
	z := atomic.LoadInt32(&p.dead)
	return z == 1
}
