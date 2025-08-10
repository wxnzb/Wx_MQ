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
	currentTerm int

	voteFor int
	//表示当前是你的官
	state int
	//要是你是leader他就很有用了
	leaderId int
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
type ResponseVoteReply struct {
	VoteGranted bool
	Term        int
}

func (r *Raft) kill() {
	atomic.StoreInt32(&r.dead, 1)
}

// 太神奇了，自己掉自己，嘻嘻
// 在你follow进行竞选的时候需要什么？竞选ID，Term
// LastLogIndex=log[len(log)-1].LogIndex,存起来确实感觉有点麻烦
func (r *Raft) SendRequestVote(server int, args *RequestVoteArgs) (*ResponseVoteReply, bool) {
	resp, err := (*r.peers[server]).RequestVote(context.Background(), &api.RequestVoteArgs_{
		Term:          int8(args.Term),
		CandidateId:   int8(args.CandidateId),
		LastLogIndex:  int8(args.LastLogIndex),
		LastLogTerm:   int8(args.LastLogTerm),
		TopicName:     r.topicName,
		PartitionName: r.partitionName,
	})
	if err != nil {
		return nil, false
	}
	return &ResponseVoteReply{
		VoteGranted: resp.VoteGranted,
		Term:        int(resp.Term),
	}, true
}
func (r *Raft) RequestVoteHandle(args *RequestVoteArgs) (reply *ResponseVoteReply) {
	r.rmu.Lock()
	defer r.rmu.Unlock()
	reply = &ResponseVoteReply{}
	//候选者的任期小于自己
	if args.Term < r.currentTerm {
		reply.VoteGranted = false
		reply.Term = r.currentTerm
		//感觉这里else后面不需要加if
	} else if args.Term >= r.currentTerm {
		if r.currentTerm < args.Term {
			r.currentTerm = args.Term
			r.voteFor = -1
			r.state = 0
			//为什么需要这个？？？
			go r.Persist()
		}
		if r.voteFor == -1 || r.voteFor == args.CandidateId {

			i := len(r.log) - 1 //索引要-1
			// 根据 Raft 的日志新旧比较规则：
			// Term 大 => 日志更新
			// Term 相同 => 再比 Index
			//if args.LastLogTerm > r.log[i].LogTerm || (args.LastLogTerm > r.log[i].LogTerm && args.LastLogIndex >= r.log[i].LogIndex) {
			if args.LastLogIndex >= r.log[i].LogIndex {
				//感觉这里思路好但是代码写的不好，不容易看懂
				if args.LastLogTerm > r.log[i].LogTerm ||
					args.LastLogIndex-r.X >= i {
					r.currentTerm = args.Term
					r.voteFor = args.CandidateId
					r.state = 0
					r.leaderId = -1
					//-----
					r.electionTimePass = 0
					rand.Seed(time.Now().UnixNano())
					r.electionTimeOut = rand.Intn(150) + 150
					reply.VoteGranted = true
				} else {
					reply.VoteGranted = false
					reply.Term = r.currentTerm
				}

			} else {
				reply.VoteGranted = false
				//感觉reply.Term就应该写成这样
				reply.Term = r.currentTerm
			}
			return ResponseVoteReply{VoteGranted: true, Term: args.Term}
		} else {
			reply.VoteGranted = false
			reply.Term = r.currentTerm
		}
	}
	go r.Persist()
	return reply
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	//Leader 日志中前一条日志的索引和任期，用于和 Follower 对齐日志。
	PrevLogIndex int
	PrevLogTerm  int
	//需要复制的日志条目
	LeaderCommit int
	//Leader 已提交的日志索引，Follower 可以用这个来更新自己的 commit index。
	Entries []LogNode
}
type AppendEntriesReply struct {
	Success bool
	Term    int
	//
	LogTerm        int
	TermFirstIndex int
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
	reply = &AppendEntriesReply{}
	if args.Term >= r.currentTerm {
		r.currentTerm = args.Term
		//这里不太理解，为什么只有大于才变成-1
		if args.Term > r.currentTine {
			r.voteFor = -1
		}
		r.state = 0
		r.leaderId = -1
		r.leaderId = args.LeaderId
		//-----
		r.electionTimePass = 0
		rand.Seed(time.Now().UnixNano())
		r.electionTimeOut = rand.Intn(150) + 150
		logs := args.Entries
		// Follower 在追加日志前必须检查本地是否：
		// 有一条日志索引等于 PrevLogIndex 的日志
		// 且那条日志的任期等于 PrevLogTerm
		//首先咱follow的log器吗得和PrevLogIndex不然前面都比一样你还想着后面
		if len(r.log)-1 >= args.PrevLogIndex-r.X && args.PrevLogIndex-r.X >= 0 {
			//那证明在pre两者term是一样的
			if args.PrevLogTerm == r.log[args.PrevLogIndex-r.X].LogTerm {
				//分情况处理，第一种，r.log长度小于prevLogIndex+len(logs),直接加就好了；第二种后面比对相同了几个之后不一样了，从不一样那里开始赋值
				index := args.PrevLogIndex - r.X + 1 //这是要进行赋值的第一个索引
				//开始遍历要加入的log
				for i, log := range logs {
					//说明现在r.log有这个长度
					if len(r.log)-1 >= index {
						//两个日志任期完全一样那么日志就是一样
						if r.log[index].LogTerm == log.LogTerm {
							index++
							//说明从这开始不等于就有要接一下
						} else {
							r.log = r.log[:index]
							r.log = append(r.log, logs[i:]...)
							r.matchIndex[r.me] = r.log[len(r.log)-1].LogIndex
							break
						}
						//没有直接加后面就行了
					} else {
						r.log = append(r.log, logs[i:]...)
						r.matchIndex[r.me] = r.log[len(r.log)-1].LogIndex
						break
					}
				}
				reply.Success = true
				//是否更新自己的 commitIndex
				//leader 告诉 follower：“我已经提交到了 LeaderCommit。”
				//而 follower 的 commitIndex 还没有这么新
				if r.commitIndex < args.LeaderCommit {
					//follower 最后一条日志的 index,是否 已经达到了 Leader 的提交点
					if r.log[len(r.log)-1].LogIndex <= args.LeaderCommit {
						//本地日志比较短 → 最多只能提交到自己的最后一条日志
						r.commitIndex = r.log[len(r.log)-1].LogIndex
					}
				} else {
					//本地日志足够新 → 可以直接跟着 Leader 提交到 LeaderCommit
					r.commitIndex = int(args.LeaderCommit)
				}
			}
		} else {
			//日志冲突
			// Raft 在日志不匹配时，会让 Follower 告诉 Leader：
			// 冲突任期 reply.Logterm；该任期的第一条日志索引 reply.Termfirstindex

			//前面都不一样当然是退出了，PrevLogIndex 超出了本地日志范围
			reply.Success = false
			if len(r.log) <= 1 {
				//要是follower是空，那么让leadre从0开始发
				reply.TermFirstIndex = 0
			} else {
				//找到冲突日志term相同的第一个日志，让leader从这里开始发送
				i := len(r.log) - 1
				reply.Term = r.log[i].LogTerm
				for i > 0 && r.log[i].LogTerm == reply.Term {
					i--
				}
				reply.TermFirstIndex = r.log[i].LogIndex + 1
			}
		}
		reply.Term = r.currentTerm
		//这里是follower的term大于leader的term
	} else {
		reply.Success = false
		reply.Term = r.currentTerm
		reply.LogTerm = 0
	}
	go r.Persist()
	return reply
}

type Per struct {
	rmu sync.RWMutex
	//----------------
	currentTerm int
	voteFor     int
	log         []LogNode
	X           int
}

// 把当前节点的 Raft 状态信息 和 快照数据 写入持久化存储，以保证宕机后能从磁盘恢复正确状态
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
	Term     int
	LeaderId int
	//快照包含的最后一条日志的索引和任期
	LastIncludedLogIndex int
	LastIncludedLogTerm  int
	//日志片段和快照数据
	Log      interface{}
	SnapShot []byte
}
type SnapShotReply struct {
	Term int
}

// 给praft用的
// 保证 follower 的状态能赶上 leader，尤其是在日志落后太多时，leader 会发送快照来替代日志的部分内容，避免同步过大日志带来的性能问题
func (r *Raft) SnapShotHandle(args *SnapShotArgs) (reply *SnapShotReply) {
	reply = &SnapShotReply{}
	r.rmu.Lock()
	defer r.rmu.Unlock()
	reply.Term = r.currentTerm
	if args.Term < r.currentTerm {
		return reply
	}
	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.state = 0
		r.voteFor = -1
		r.leaderId = args.LeaderId
		r.electionTimePass = 0
		rand.Seed(time.Now().UnixNano())
		r.electionTimeOut = rand.Intn(150) + 150
		go r.Persist()
	}
	// 如果我们已经有更新的快照或日志，不需要安装
	if args.LastIncludedLogIndex <= r.X {
		return reply
	}
	// 处理 snapshot 覆盖 log 的情况
	// 情况 1：snapshot 超出我们 log 的最后，全部丢弃
	if args.LastIncludedLogIndex > r.log[len(r.log)-1].LogIndex {
		r.log = []LogNode{
			{
				LogIndex: args.LastIncludedLogIndex,
				LogTerm:  args.LastIncludedLogTerm,
				Log:      args.Log,
			},
		}
		// 情况 2：snapshot 包含我们一部分 log，截断前面的
	} else {
		first := 1
		for i, log := range r.log {
			//找到了快照的那个点
			if log.LogIndex == args.LastIncludedLogIndex && log.LogTerm == args.LastIncludedLogTerm {
				first = i
				break
			}
			if first != -1 {
				r.log = r.log[first+1:]
				r.log = append([]LogNode{
					{
						LogIndex: args.LastIncludedLogIndex,
						LogTerm:  args.LastIncludedLogTerm,
						Log:      args.Log,
					}}, r.log...)
				//根本就没有和快照一样的点
			} else {
				r.log = []LogNode{
					{
						LogIndex: args.LastIncludedLogIndex,
						LogTerm:  args.LastIncludedLogTerm,
						Log:      args.Log,
					},
				}
			}
		}
	}
	//更新 snapshot 元信息
	r.X = args.LastIncludedLogIndex
	r.lastIndex = r.X
	r.lastTerm = args.LastIncludedLogTerm
	r.snapshot = args.SnapShot
	//更新状态推进
	if r.commitIndex < r.X {
		r.commitIndex = r.X
	}
	if r.lastApplied < r.X {
		r.lastApplied = r.X
	}
	go r.Persist()
	return reply
}
func (r *Raft) sendSnapShot(server int, args *SnapShotArgs) (*SnapShotReply, bool) {
	resp, err := (*r.peers[server]).SnapShot(context.Background(), &api.SnapShotArgs_{
		Term:                 int8(args.Term),
		LeaderId:             int8(args.LeaderId),
		LastIncludedLogIndex: int8(args.LastIncludedLogIndex),
		LastIncludedLogTerm:  int8(args.LastIncludedLogTerm),
		//Log:                args.Log,
		SnapShot:      args.SnapShot,
		TopicName:     r.topicName,
		PartitionName: r.partitionName,
	})
	if err != nil {
		return nil, false
	}
	return &SnapShotReply{
		Term: int(resp.Term),
	}, true
}

// leader 节点调用的 SendSnapshot
// Leader 发现某个 Follower 太落后了，无法通过追加日志追上自己，于是干脆给它发一整个“快照”过去，让它一下子同步到最新状态
func (r *Raft) SendSnapShot(term, it int) {
	args := &SnapShotArgs{}
	r.rmu.Lock()
	args.Term = term
	args.LeaderId = r.me
	args.LastIncludedLogIndex = r.lastIndex
	args.LastIncludedLogTerm = r.lastTerm
	args.Log = r.log[0].Log
	args.SnapShot = r.snapshot
	r.rmu.Unlock()
	reply, ok := r.sendSnapShot(it, args)
	if ok {
		r.rmu.Lock()
		if reply.Term > r.currentTerm {
			r.currentTerm = reply.Term
			r.state = 0
			r.voteFor = -1
			r.leaderId = -1
			go r.Persist()
			r.electionTimePass = 0
			rand.Seed(time.Now().UnixNano())
			r.electionTimeOut = rand.Intn(150) + 150
		}
		r.rmu.Unlock()
	}
}

// 接下来随便写点关于raft的函数把，这些是还没有调用的
func Make(clients []*raft_operations.Client, me int, persist *Persister, applyCh chan ApplyMsg, topicName string, partitionName string) *Raft {
	r := &Raft{
		rmu:         sync.RWMutex{},
		peers:       clients,
		persist:     persist,
		currentTerm: 0,
		voteFor:     -1,
		//表示当前是你的官
		state: 0,
		//要是你是leader他就很有用了
		leaderId: -1,
		log:      []LogNode{},
		//electionTimeOut  int
		electionTimePass: 0,
		durX:             0,
		X:                0,
		commitIndex:      0,
		me:               me,
		//当前已经应用到状态机的最大索引
		lastApplied: 0,
		//--------------------------
		topicName:     topicName,
		partitionName: partitionName,
		//---------------
		//snapshot      []byte
	}
	//r.cond = sync.Cond(&r.rmu)
	r.cond = *sync.NewCond(r.rmu.RLocker())
	for i := 0; i < len(r.peers); i++ {
		r.nextIndex = append(r.nextIndex, 1)
		r.matchIndex = append(r.matchIndex, 0)
	}
	r.log = append(r.log, LogNode{
		LogIndex: 0,
	})
	rand.Seed(time.Now().UnixNano())
	r.electionTimeOut = rand.Intn(150) + 150
	startIndex := r.X
	go r.Commited(startIndex, applyCh)
	LOGinit()
	go r.ReadPersister(r.persist.GetRaftState(), r.persist.GetSnapShot())
	go r.ticker()
	return r
}

// Raft 节点从持久化状态中恢复自身状态
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
		//“快照截断”时最后保留的那条日志 index
		r.lastIndex = r.X
		//当前已经提交的最后一条日志 index”，快照意味着我们至少已经 commit 到 index=3 了
		r.commitIndex = r.X
		r.lastApplied = r.X
		//一般log[0]里面存放的是快照
		r.lastTerm = int(r.log[0].LogTerm)
		r.matchIndex[r.me] = r.log[len(r.log)-1].LogIndex
		r.rmu.Unlock()
	}
}

type ApplyMsg struct {
	CommandValid bool
	Command      Op
	CommandIndex int
	//--------------------
	TopicName string
	PartName  string
	//我是不是这个 partition 的 leader？
	BeLeader bool
	Leader   int
	//-------------------
	SnapshotValid bool
	Snapshot      []byte
	//表示这个快照覆盖到哪一条日志（index）、term 是多少。
	SnapshotTerm  int
	SnapshotIndex int
}
type Op struct {
	Cli_name  string
	Cmd_index int64
	Ser_index int64
	Operate   string
	Tpart     string
	Topic     string
	Part      string
	Num       int
	Msg       []byte
	Size      int8
}
type LogNode struct {
	LogTerm  int
	LogIndex int
	Log      interface{}
	BeLeader bool
	Leader   int
}

// 将已经 committed 的日志应用（apply）到状态机
func (r *Raft) Commited(startIndex int, applyCh chan ApplyMsg) {
	for !r.killed() {
		r.rmu.Lock()
		//状态机还没应用快照
		if len(r.log) > 0 && startIndex < r.X {
			node := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      r.snapshot,
				//这里的last？？
				//快照的最后一条日志 index == rf.X
				//快照中最后一条日志的任期
				SnapshotTerm:  r.lastTerm,
				SnapshotIndex: r.lastIndex,
			}
			startIndex = r.X
			//应用快照这个就得这样赋值
			r.lastApplied = r.lastIndex
			r.rmu.Unlock()
			applyCh <- node
		} else {
			r.rmu.Unlock()
		}
		r.rmu.Lock()
		var arry []LogNode
		//当前已经提交（可供状态机 apply）的最大日志 index
		commit := r.commitIndex - r.X
		//当前已经被 apply 到状态机的最大日志 index
		applied := r.lastApplied - r.X
		if commit > applied && applied >= 0 && commit <= len(r.log)-1 {
			arry = r.log[applied+1 : commit+1]
		}
		r.rmu.Unlock()
		if commit > applied {
			for _, it := range arry {
				node := ApplyMsg{
					CommandValid: true,
					CommandIndex: it.LogIndex,
					Command:      it.Log,
					BeLeader:     it.BeLeader,
				}
				if node.BeLeader {
					node.TopicName = r.topicName
					node.PartName = r.partitionName
					node.Leader = it.Leader
				}
				r.rmu.Lock()
				r.lastApplied++
				r.rmu.Unlock()
				applyCh <- node
			}
			go r.Persist()
		}
		time.Sleep(time.Microsecond * 20)
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
				//go r.appendentries(r.currentTerm)
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
func (r *Raft) appendentries(term int) {
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
			go func(p int, t int) {
				args := &AppendEntriesArgs{}
				args.Term = term
				args.LeaderId = int(r.me)
				r.rmu.Lock()
				//因为上面他把锁解开了，所以在并发条件下可能修改了，因此有的要重复进行判断
				// 现在开始判断不符合条件的直接退出就行
				//要是上一次记录的快照和当前leader不一样或者index也不一样那，说明更新了
				if r.durX != r.X || index != len(r.log)-1 {
					r.durX = r.X
					r.rmu.Unlock()
					wg.Done()
					return
				}
				//最后一条日志
				if index == len(r.log)-1 {
					//出现这种情况的原因是快照清掉了日志前缀，也就是检查当前i是否存在
					if int(i)-r.X >= 0 && r.log[int(i)-r.X].LogIndex != i {
						r.rmu.Unlock()
						wg.Done()
						return
					}
				}
				//term或者和state在并发下改变了
				if r.currentTerm != t || r.state != 2 {
					r.rmu.Unlock()
					wg.Done()
					return
				}
				//这个的作用，在发送给follower是，告诉Follower要发送的条目的上一个看能和follower对应上不
				//leader 1 2 3 4 5,现在要发送4 5
				//follower 1 2 3
				//那么都是3就可以对上
				//这个是用于一致性检查，这里不用考虑r.X吗
				args.PrevLogIndex = r.log[r.nextIndex[p]-1].LogIndex
				args.PrevLogTerm = r.log[r.nextIndex[p]-1].LogTerm
				//Leader 确认有多数 Follower 拥有该日志条目后，可以提交并应用到状态机
				args.LeaderCommit = int(r.commitIndex)
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
				} else {
					if reply.Term > r.currentTerm {
						r.currentTerm = reply.Term
						r.voteFor = -1
						r.state = 0
						r.leaderId = -1
						//-----
						r.electionTimePass = 0
						rand.Seed(time.Now().UnixNano())
						r.electionTimeOut = rand.Intn(150) + 150
						//2是什么状态？？
						//这个分支不太明白
					} else if r.state == 2 {
						if int(reply.TermFirstIndex) < r.X {
							go r.SendSnapShot(r.currentTerm, i)
						} else if int(reply.TermFirstIndex)-r.X > 1 {
							r.nextIndex[p] = int(reply.TermFirstIndex) - r.X
							if r.nextIndex[p] > len(r.log) {
								r.nextIndex[p] = len(r.log)
							}
						} else {
							r.nextIndex[p] = 1
						}
					}
				}
				wg.Done()
			}(p, t)
		}
	}
	wg.Wait()
}

//	type SnapShotArgs struct {
//		Term     int
//		LeaderId int
//		//快照包含的最后一条日志的索引和任期
//		LastIncludedLogIndex int
//		LastIncludedLogTerm  int
//		//日志片段和快照数据
//		Log      interface{}
//		SnapShot []byte
//	}
//
//	type SnapShotReply struct {
//		Term int
//	}
func (r *Raft) sendsnapshot(term int, it int) {
	args := SnapShotArgs{}
	r.rmu.Lock()
	args.Term = term
	args.LeaderId = r.me
	args.LastIncludedLogIndex = r.lastIndex
	args.LastIncludedLogTerm = r.lastTerm
	args.Log = r.log[0].Log
	r.rmu.Unlock()
	reply, ok := r.SendSnapShot(it, &args)
	if ok {
		r.rmu.Lock()
		if reply.Term > r.currentTerm {
			r.currentTerm = reply.Term
			r.voteFor = -1
			r.state = 0
			r.leaderId = -1
			r.electionTimePass = 0
			rand.Seed(time.Now().UnixNano())
			r.electionTimeOut = rand.Intn(150) + 150
		}
		r.rmu.Unlock()
	}
}
func (r *Raft) requestvotes(term int) {
	r.rmu.Lock()
	peers := len(r.peers)
	r.voteFor = r.me
	var wg sync.WaitGroup
	wg.Add(len(r.peers) - 1)
	VoteGrantedNum := int64(1)
	r.rmu.Unlock()
	for it := range r.peers {
		if it != r.me {
			go func(it, term int) {
				args := &RequestVoteArgs{}
				args.Term = term
				args.CandidateId = r.me
				args.LastLogIndex = r.log[len(r.log)-1].LogIndex
				args.LastLogTerm = r.log[len(r.log)-1].LogTerm
				reply, ok := r.SendRequestVote(it, args)
				if ok {
					r.rmu.Lock()
					if term != r.currentTerm {
						//说明错误
					} else if r.state == 1 {
						if reply.VoteGranted {
							atomic.AddInt64(&VoteGrantedNum, 1)
						}
						//说明成功当成leader了
						if VoteGrantedNum > int64(peers)/2 {
							r.state = 2
							r.electionTimeOut = 90
							r.electionTimePass = 0
							//下面这还需详细了解
							r.matchIndex[r.me] = r.log[len(r.log)-1].LogIndex
							for i := 0; i < len(r.peers); i++ {
								//这里是在干什么
								r.nextIndex[i] = len(r.log)
								if i != r.me {
									r.matchIndex[i] = 0
								}
							}
							go r.Start(Op{
								Cli_name: "leader",
								Topic:    r.topicName,
								Tpart:    r.topicName + r.partitionName,
								Part:     r.partitionName,
							}, true, r.me)
							//go r.appendentries(term)
						}
						//我就很好器，不是已经ok了吗，怎么还会有这种问题？
						if reply.Term > r.currentTerm {
							r.state = 0
							r.currentTerm = reply.Term
							r.voteFor = -1
							r.leaderId = -1
							r.electionTimePass = 0
							rand.Seed(time.Now().UnixNano())
							r.electionTimeOut = rand.Intn(150) + 150
						}
						go r.Persist()
					}
					r.rmu.Unlock()
				} else {
					//报错
				}
				wg.Done()
			}(it, term)
		}
	}
	wg.Wait()
}

// 如果当前节点是 Leader，就把客户端传入的 command 封装成日志项，追加到本地日志中，然后触发 Raft 的日志复制过程
func (r *Raft) Start(command Op, beleader bool, leader int) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	if !r.killed() {

		r.rmu.Lock()
		if r.state == 2 {
			isLeader = true
			com := LogNode{
				LogTerm:  r.currentTerm,
				LogIndex: len(r.log) + r.X,
				Log:      command,
				BeLeader: beleader,
				Leader:   leader,
			}
			r.log = append(r.log, com)
			//matchIndex 表示该节点上已复制的最新日志索引
			r.matchIndex[r.me]++

			index = com.LogIndex
			term = r.currentTerm
			DEBUG(dLog, "S%d have log %v\n", r.me, com)
			//进行持久化
			go r.Persist()
			r.electionTimePass = 0
			//启动日志同步
			go r.appendentries(r.currentTerm)
		}
		r.rmu.Unlock()
	}
	return index, term, isLeader
}

// 这些感觉还没有用上
// 线程安全地在 Raft 节点日志中查找指定索引的日志命令内容
func (r *Raft) Find(in int) interface{} {
	var logs []LogNode
	r.rmu.Lock()
	logs = append(logs, r.log...)
	r.rmu.Unlock()
	for _, log := range logs {
		if log.LogIndex == in {
			return log.Log
		}
	}
	return nil
}

// 让外部系统可以获取当前这个 Raft 节点的状态：它的任期 term 和是否是 leader 的布尔值
func (r *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	r.rmu.Lock()
	term = r.currentTerm
	if r.state == 2 {
		isLeader = true
	}
	r.rmu.Unlock()
	return term, isLeader
}

// 获取当前快照触发条件相关的状态信息：一个是 X 的值（用于自定义触发快照的逻辑），另一个是当前 Raft 状态（包括日志和快照）在持久化存储中占用的字节大小
func (r *Raft) RaftSize() (int, int) {
	r.rmu.Lock()
	XSize := r.X
	r.rmu.Unlock()
	return XSize, r.persist.GetRaftSize()
}

// 将当前 Raft 实例标记为“已终止”，
func (r *Raft) Kill() {
	atomic.StoreInt32(&r.dead, 1)
}
