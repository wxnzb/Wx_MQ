package raft

import (
	"Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/raft_operations"
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// 在快照恢复时，日志结构一般是这样设计的：
// rf.log = []LogEntry{
//     {LogIndex: X, LogTerm: XTerm},  // 占位符，占据 log[0]，对应 snapshot.LastIncludedIndex
//     {LogIndex: X+1, LogTerm: ...},
//     {LogIndex: X+2, LogTerm: ...},

// }
type Raft struct {
	rmu         sync.RWMutex
	cond        sync.Cond
	currentTerm int8

	voteFor int8
	//表示当前是你的官
	state int8
	//要是你是leader他就很有用了
	leaderId int8
	//在raft中，无论是leadre还是follow都是节点，me代表的是当前节点的id
	me int
	//leader 将要发送给节点 i 的下一个日志条目的索引（即：还没复制的第一条日志 index）
	//节点（包括 leader)当前复制到了哪个日志索引
	nextIndex []int
	//follower已经成功复制的最大索引
	matchIndex       []int
	log              []LogNode
	electionTimeOut  int
	electionTimePass int
	//这是某一时间的快照
	durX int
	X    int
	//对于follew来说就是；对于leader来说就是看follower要是都有了就提交给状态机
	commitIndex int
	//---------------
	snapshot      []byte
	persist       *Persister
	peers         []*raft_operations.Client
	topicName     string
	partitionName string
	//--------------------
	//本节点当前日志中最后一条日志的索引
	lastIndex int
	//当前已经应用到状态机的最大索引
	lastApplied int
	//lastIndex 对应日志的任期（Term）
	lastTerm int
	dead     int32
}

type RequestVoteArgs struct {
	Term         int8
	CandidateId  int8
	LastLogIndex int8
	LastLogTerm  int8
}
type ResponseVoteReply struct {
	VoteGranted bool
	Term        int8
}

// 太神奇了，自己掉自己，嘻嘻
func (r *Raft) SendRequestVote(server int, args *RequestVoteArgs) (*ResponseVoteReply, bool) {
	resp, err := (*r.peers[server]).RequestVote(context.Background(), &api.RequestVoteArgs_{
		Term:          args.Term,
		CandidateId:   args.CandidateId,
		LastLogIndex:  args.LastLogIndex,
		LastLogTerm:   args.LastLogTerm,
		TopicName:     r.topicName,
		PartitionName: r.partitionName,
	})
	if err != nil {
		return nil, false
	}
	return &ResponseVoteReply{
		VoteGranted: resp.VoteGranted,
		Term:        resp.Term,
	}, true
}
func (r *Raft) RequestVoteHandle(args *RequestVoteArgs) ResponseVoteReply {
	r.rmu.Lock()
	defer r.rmu.Unlock()
	if args.Term < r.currentTerm {
		return ResponseVoteReply{VoteGranted: false, Term: r.currentTerm}
	} else {
		r.currentTerm = args.Term
		r.voteFor = -1
		r.state = 0
	}
	if r.voteFor == -1 || r.voteFor == args.CandidateId {

		i := len(r.log) - 1 //索引要-1
		if args.LastLogTerm > r.log[i].LogTerm || (args.LastLogTerm > r.log[i].LogTerm && args.LastLogIndex >= r.log[i].LogIndex) {
			r.currentTerm = args.Term
			r.voteFor = args.CandidateId
			r.state = 0
			r.leaderId = -1
			//-----
			r.electionTimePass = 0
			rand.Seed(time.Now().UnixNano())
			r.electionTimeOut = rand.Intn(150) + 150
		} else {
			return ResponseVoteReply{VoteGranted: false, Term: args.Term}
		}

	} else {
		return ResponseVoteReply{VoteGranted: false, Term: args.Term}
	}
	return ResponseVoteReply{VoteGranted: true, Term: args.Term}
}

type LogNode struct {
	LogTerm  int8
	LogIndex int8
	Log      interface{}
}
type AppendEntriesArgs struct {
	Term     int8
	LeaderId int8
	//Leader 日志中前一条日志的索引和任期，用于和 Follower 对齐日志。
	PrevLogIndex int8
	PrevLogTerm  int8
	//需要复制的日志条目
	LeaderCommit int8
	//Leader 已提交的日志索引，Follower 可以用这个来更新自己的 commit index。
	Entries []LogNode
}
type AppendEntriesReply struct {
	Success bool
	Term    int8
	//
	LogTerm        int8
	TermFirstIndex int8
}

// 发送心跳包
func (r *Raft) SendAppendEntries(server int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	data, _ := json.Marshal(args.Entries)
	resp, err := (*r.peers[server]).AppendEntries(context.Background(), &api.AppendEntriesArgs_{
		Term:          args.Term,
		LeaderId:      args.LeaderId,
		PrevLogIndex:  args.PrevLogIndex,
		PrevLogTerm:   args.PrevLogTerm,
		LeaderCommit:  args.LeaderCommit,
		Entries:       data,
		TopicName:     r.topicName,
		PartitionName: r.partitionName,
	},
	)
	if err != nil {
		return nil, false
	}
	return &AppendEntriesReply{
		Success:        resp.Success,
		Term:           resp.Term,
		LogTerm:        resp.LogTerm,
		TermFirstIndex: resp.TermFirstIndex,
	}, true
}

// leader向follow添加新日志
func (r *Raft) AppendEntriesHandle(args *AppendEntriesArgs) (reply *AppendEntriesReply) {
	r.rmu.Lock()
	defer r.rmu.Unlock()
	//这里方的有问题
	go r.Persist()
	if args.Term >= r.currentTerm {
		r.currentTerm = args.Term
		r.voteFor = -1
		r.state = 0
		r.leaderId = -1
		r.leaderId = args.LeaderId
		//-----
		r.electionTimePass = 0
		rand.Seed(time.Now().UnixNano())
		r.electionTimeOut = rand.Intn(150) + 150
		logs := args.Entries
		//首先咱follow的log器吗得和PrevLogIndex不然前面都比一样你还想着后面
		if len(r.log)-1 >= int(args.PrevLogIndex)-r.X && int(args.PrevLogIndex)-r.X >= 0 {
			//那证明在pre两者term是一样的
			if args.PrevLogTerm == r.log[int(args.PrevLogIndex)-r.X].LogTerm {
				//分情况处理，第一种，r.log长度小于prevLogIndex+len(logs),直接加就好了；第二种后面比对相同了几个之后不一样了，从不一样那里开始赋值
				index := int(args.PrevLogIndex) - r.X + 1 //这是要进行赋值的第一个索引
				//开始遍历要加入的log
				for i, log := range logs {
					//说明现在r.log有这个长度
					if len(r.log)-1 >= index {
						if r.log[index].LogTerm == log.LogTerm {
							//说明从这开始不等于就有要接一下
						} else {
							r.log = r.log[:index]
							r.log = append(r.log, logs[i:]...)
							break
						}
						//没有直接加后面就行了
					} else {
						r.log = append(r.log, logs[i:]...)
						break
					}
				}
				reply.Success = true
				//提交日志，虽然不知道这个是干啥的
				//更新 follower 的 commitIndex，确保它不会超过 leader 已经提交的日志条目，但也不会超过自己本地日志的最大索引
				//这里制作一个判断的原因：上面已经修改了
				//这里i还是没太看明白，感觉这段代码逻辑有点问题
				if r.commitIndex < int(args.LeaderCommit) {
					if r.log[len(r.log)-1].LogIndex > args.LeaderCommit {
						r.commitIndex = int(r.log[len(r.log)-1].LogIndex)
					}
				} else {
					r.commitIndex = int(args.LeaderCommit)
				}
			}
		} else {
			// Raft 在日志不匹配时，会让 Follower 告诉 Leader：
			// 冲突任期 reply.Logterm；该任期的第一条日志索引 reply.Termfirstindex

			//前面都不一样当然是退出了，PrevLogIndex 超出了本地日志范围
			reply.Success = false
			if len(r.log) <= 1 {
				reply.TermFirstIndex = 0
			} else {
				i := len(r.log) - 1
				reply.Term = r.log[i].LogTerm
				for i > 0 && r.log[i].LogTerm == reply.Term {
					i--
				}
				reply.TermFirstIndex = r.log[i].LogIndex + 1
			}
		}
	} else {
		reply.Success = false
		reply.Term = r.currentTerm
	}
	return
}

type Per struct {
	rmu sync.RWMutex
	//----------------
	currentTerm int8
	voteFor     int8
	log         []LogNode
	X           int
}

func (r *Raft) Persist() {
	w := new(bytes.Buffer)
	e := NewEncoder(w)
	var user Per
	r.rmu.Lock()
	user.currentTerm = r.currentTerm
	user.voteFor = r.voteFor
	user.log = r.log
	user.X = r.X
	e.Encode(user)
	state := w.Bytes()
	r.rmu.Unlock()
	//这个又须烤烟
	Snapshot := r.snapshot
	go r.persist.XGRaftstateAndSnapshot(state, Snapshot)
}

type SnapShotArgs struct {
	Term     int8
	LeaderId int8
	//快照包含的最后一条日志的索引和任期

	LastIncludedLogIndex int8
	LastIncludedLogTerm  int8
	//日志片段和快照数据
	Log      interface{}
	SnapShot []byte
}
type SnapShotReply struct {
	Term int8
}

func (r *Raft) SendSnapShot(server int, args *SnapShotArgs) (*SnapShotReply, bool) {
	resp, err := (*r.peers[server]).SnapShot(context.Background(), &api.SnapShotArgs_{
		Term:                 args.Term,
		LeaderId:             args.LeaderId,
		LastIncludedLogIndex: args.LastIncludedLogIndex,
		LastIncludedLogTerm:  args.LastIncludedLogTerm,
		//Log:                args.Log,
		SnapShot:      args.SnapShot,
		TopicName:     r.topicName,
		PartitionName: r.partitionName,
	})
	if err != nil {
		return nil, false
	}
	return &SnapShotReply{
		Term: resp.Term,
	}, true
}

// 先不写这个了，态麻烦了
// 保证 follower 的状态能赶上 leader，尤其是在日志落后太多时，leader 会发送快照来替代日志的部分内容，避免同步过大日志带来的性能问题
// func (r *Raft) SnapShotHandle(args *SnapShotArgs) (reply *SnapShotReply) {

// }

// 接下来随便写点关于raft的函数把，这些是还没有调用的
func Make(clients []*raft_operations.Client, me int, persist *Persister, topicName string, partitionName string) *Raft {
	r := &Raft{
		peers:         clients,
		persist:       persist,
		topicName:     topicName,
		partitionName: partitionName,
		currentTerm:   0,
		voteFor:       -1,
		//表示当前是你的官
		state: 0,
		//要是你是leader他就很有用了
		leaderId: -1,
		log:      []LogNode{},
		//electionTimeOut  int
		electionTimePass: 0,
		X:                0,
		commitIndex:      0,
		me:               me,
		//---------------
		//snapshot      []byte
	}
	r.cond = sync.Cond(&r.rmu)
	for i := 0; i < len(r.peers); i++ {
		r.nextIndex = append(r.nextIndex, 1)
		r.matchIndex = append(r.matchIndex, 0)
	}
	r.log = append(r.log, LogNode{
		LogIndex: 0,
	})
	rand.Seed(time.Now().UnixNano())
	r.electionTimeOut = rand.Intn(150) + 150
	LOGinit()
	go r.ReadPersister(r.persist.GetRaftState(), r.persist.GetSnapShot())
	go r.ticker()
	return r
}
func (r *Raft) ReadPersister(state []byte, snapshot []byte) {
	if state == nil || len(state) < 1 {
		return
	}

	p := bytes.NewBuffer(state)
	d := NewDecoder(p)
	var user Per
	if d.Decode(&user) != nil {
		return
	} else {
		r.rmu.Lock()
		r.currentTerm = user.currentTerm
		r.voteFor = user.voteFor
		r.log = user.log
		r.X = user.X
		//-----------------------
		r.snapshot = snapshot
		//这些都是什么，为什么要赋值r.X
		r.lastIndex = r.X
		r.commitIndex = r.X
		r.lastApplied = r.X
		//一般log[0]里面存放的是快照
		r.lastTerm = int(r.log[0].LogTerm)

		r.rmu.Unlock()
	}

}

// 这个主要是在timepass>timeout是对于leadre和candiddate的处理
func (r *Raft) ticker() {
	for !r.killed() {
		r.rmu.Lock()
		if r.electionTimePass > r.electionTimeOut {
			//重置
			r.electionTimePass = 0
			rand.Seed(time.Now().UnixNano())
			r.electionTimeOut = rand.Intn(150) + 150
			//是leader
			if r.state == 2 {
				//是follow，但是他没有在固定时间内收到leader的心跳，变成candidata
				//上面设置了，这个为什么还要设置
				r.electionTimeOut = 90
				go r.Persist()
				//开始发送心跳,这个还没写
				go r.appendentries(r.currentTerm)
			} else {
				r.currentTerm++
				r.voteFor = -1
				r.state = 1
				go r.Persist()
				go r.requestvotes(r.currentTerm)
			}
		}
		r.electionTimePass++
		r.rmu.Unlock()
		time.Sleep(time.Millisecond)
	}
}
func (r *Raft) killed() bool {
	z := atomic.LoadInt32(&r.dead)
	return z == 1
}

// 这个是leader调用的
func (r *Raft) appendentries(term int8) {
	var wg sync.WaitGroup
	r.rmu.Lock()
	index := len(r.log) - 1
	i := r.log[index].LogIndex
	t := r.log[index].LogTerm
	l := len(r.peers)
	r.rmu.Unlock()
	//不包含他自己
	wg.Add(l - 1)
	for p := range r.peers {
		//不要自己这个leader节点才好
		if p != r.me {
			go func(p int, t int8) {
				args := &AppendEntriesArgs{}
				args.Term = term
				args.LeaderId = int8(r.me)
				r.rmu.Lock()
				//因为上面他把锁解开了，所以在并发条件下可能修改了，因此有的要重复进行判断
				// 现在开始判断不符合条件的直接退出就行
				//快照或者log更新
				if r.durX != r.X || index != len(r.log)-1 {
					r.durX = r.X
					r.rmu.Unlock()
					wg.Done()
				}
				//最后一条日志
				if index == len(r.log)-1 {
					//出现这种情况的原因是快照清掉了日志前缀
					if int(i)-r.X >= 0 && r.log[int(i)-r.X].LogIndex != i {
						r.rmu.Unlock()
						wg.Done()
					}
				}
				//term或者和state在并发下改变了
				if r.currentTerm != t || r.state != 2 {
					r.rmu.Unlock()
					wg.Done()
				}
				//这个的作用，在发送给follower是，告诉Follower要发送的条目的上一个看能和follower对应上不
				//leader 1 2 3 4 5,现在要发送4 5
				//follower 1 2 3
				//那么都是3就可以对上
				//这个是用于一致性检查
				args.PrevLogIndex = r.log[r.nextIndex[p-1]].LogIndex
				args.PrevLogTerm = r.log[r.nextIndex[p-1]].LogTerm
				//Leader 确认有多数 Follower 拥有该日志条目后，可以提交并应用到状态机
				args.LeaderCommit = int8(r.commitIndex)
				//Leader 从自己的日志中提取需要发送给某个 Follower 的日志条目（Entries），并放入 AppendEntriesArgs 中，准备发送给对应的 Follower
				//这个 Follower 确实有日志没有同步，而且不太理解后面的
				if len(r.log)-1 >= r.nextIndex[p] && i+1 > r.log[r.nextIndex[p]].LogIndex {
					nums := r.log[int(r.log[r.nextIndex[p]].LogIndex)-r.X : int(i)-r.X+1]
					args.Entries = append(args.Entries, nums...)
				}
				r.rmu.Unlock()
				reply, ok := r.SendAppendEntries(p, args)
				if ok {
					r.rmu.Lock()
					//首先就是传送成功，那么就要更新follower的matchIndex,和nextIndex
					if reply.Success == true {
						r.matchIndex[p] = int(i)
						//i:当前 leader 认为 follower 已经成功复制的日志 index
						//判断是否存在快照截断，感觉可以简化成这样
						if r.durX == r.X {
							if int(i)-r.X+1 >= 0 {
								r.nextIndex[p] = int(i) - r.X + 1
							} else {
								r.nextIndex[p] = 1
							}
							//存在快照截断
						} else {
							if int(i) >= r.X {
								r.nextIndex[p] = int(i) - r.X + 1
							} else {
								r.nextIndex[p] = 1
							}
							r.durX = r.X
						}
						//leader满足条件就可以提交日知道状态机上面
						successCommit := 0
						for in := range r.matchIndex {
							if in >= int(i) {
								successCommit++
							}
						}
						//i和commitIndex区别：i:leader 推送给某个 follower 的日志条目的位置（在日志数组中的 offset）
						//commitIndex:就是已经给大多数follower更新了也已经提交到状态机了，上面这个就是下一次的下面这个状态
						if successCommit > l/2 && r.currentTerm == t && int(i) > r.commitIndex-r.X {
							r.commitIndex = int(i)
						}
					}
				}
			}(p, t)
		}
	}
}
