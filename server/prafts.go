package server

import (
	api "Wx_MQ/kitex_gen/api"
	"Wx_MQ/kitex_gen/api/raft_operations"
	"context"
	"net"
	"sync"

	"Wx_MQ/raft"

	"encoding/json"

	"github.com/cloudwego/kitex/server"
)

type parts_raft struct {
	rmu      sync.RWMutex
	srv_raft server.Server
	parts    map[string]*raft.Raft
}

func NewPartRaft() *parts_raft {
	return &parts_raft{
		rmu:   sync.RWMutex{},
		parts: make(map[string]*raft.Raft),
	}
}
func (praft *parts_raft) make(name string, hostport string) error {
	addr, _ := net.ResolveTCPAddr("tcp", hostport)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))
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
