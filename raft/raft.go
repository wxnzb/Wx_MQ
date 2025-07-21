package raft

type Raft struct {
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

func (r *Raft) RequestVoteHandle(args *RequestVoteArgs) ResponseVoteReply {

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

func (r *Raft) AppendEntriesHandle(args *AppendEntriesArgs) AppendEntriesReply {

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

func (r *Raft) SnapShotHandle(args *SnapShotArgs) SnapShotReply {}
