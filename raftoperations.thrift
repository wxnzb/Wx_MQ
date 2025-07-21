namespace go api
//没有leader或者leader宕机是，需要在candadate里面选举一个
struct RequestVoteArgs{
    1:i8 Term
    2:i8 CandidateId
    3:i8 LastLogIndex
    4:i8 LastLogTerm
    5:string TopicName
    6:string PartitionName
}
struct ResponseVoteReply{
    1:bool VoteGranted
    2:i8 Term
}
//Leader 节点发起：日志复制：把新的日志条目发送给 Follower。
struct AppendEntriesArgs{
    1:i8 Term
    2:i8 LeaderId
    //Leader 日志中前一条日志的索引和任期，用于和 Follower 对齐日志。
    3:i8 PrevLogIndex
    4:i8 PrevLogTerm
    //需要复制的日志条目
    5:i8 LeaderCommit
    //Leader 已提交的日志索引，Follower 可以用这个来更新自己的 commit index。
    6:binary Entries
    7:string TopicName
    8:string PartitionName
}
struct AppendEntriesReply{
    1:bool Success
    2:i8 Term
    //
    3: i8 LogTerm
    4:i8 TermFirstIndex
}

//当日志太长时，Raft 节点会创建一个 快照（snapshot），把历史日志压缩成一个快照文件。
//当某些 Follower 落后太多日志时，Leader 不再发送旧日志，而是直接发送快照数据
struct SnapShotArgs{
    1:i8 Term
    2:i8 LeaderId
    //快照包含的最后一条日志的索引和任期

    3:i8 LastIncludedLogIndex
    4:i8 LastIncludedLogTerm
    //日志片段和快照数据
    5:binary Log
    6:binary SnapShot
    7:string TopicName
    8:string PartitionName
}
struct SnapShotReply{
    1:i8 Term
}
service Raft_Operations{
    ResponseVoteReply RequestVote(1:RequestVoteArgs args),
    AppendEntriesReply AppendEntries(1:AppendEntriesArgs args),
    SnapShotReply SnapShot(1:SnapShotArgs args)
}