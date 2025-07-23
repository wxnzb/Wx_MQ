package raft

import (
	"Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/raft_operations"
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"time"
)

type Raft struct {
	rmu         sync.RWMutex
	currentTerm int8

	voteFor int8
	//表示当前是你的官
	state int8
	//要是你是leader他就很有用了
	leaderId         int8
	log              []LogNode
	electionTimeOut  int
	electionTimePass int
	X                int
	commitIndex      int8
	//---------------
	snapshot      []byte
	persist       *Persister
	peers         []*raft_operations.Client
	topicName     string
	partitionName string
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
				if r.commitIndex < args.LeaderCommit {
					if r.log[len(r.log)-1].LogIndex > args.LeaderCommit {
						r.commitIndex = r.log[len(r.log)-1].LogIndex
					}
				} else {
					r.commitIndex = args.LeaderCommit
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
func (r *Raft) SnapShotHandle(args *SnapShotArgs) (reply *SnapShotReply) {

}

// 接下来随便写点关于raft的函数把，这些是还没有调用的
func Make(clients []*raft_operations.Client, persist *Persister, topicName string, partitionName string) *Raft {
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
		//---------------
		//snapshot      []byte
	}
	r.log = append(r.log, LogNode{
		LogIndex: 0,
	})
	rand.Seed(time.Now().UnixNano())
	r.electionTimeOut = rand.Intn(150) + 150
	LOGinit()
	go r.ReadPersister(r.persist.GetRaftState(), r.persist.GetSnapShot())
	return r
}
func (r *Raft) ReadPersister(state []byte, snapshot []byte) {

}
