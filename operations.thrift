namespace go api
// 推送消息
//下面的key是分区
struct PushRequest{
    1:string producerId

    2:string topic
    3:string key

    4:binary message
    //虾米在哪这两个会用到吗
    5:i64 StartIndex
    6:i64 EndIndex
    7:i8 Size

    8:i8 Ack
    9:i64 Comindex
}
//推送消息是否成功
struct PushResponse{
    1:bool ret
    2:string err
}
//拉取消息所需的信息
struct PullRequest{
    1:string consumerId

    2:string topic
    3:string key

    4:i64 offset
    5:i8 size

    6:i8 option
}
//拉取到的消息
struct PullResponse{
    1:binary Msgs
    2:bool Ret
    3:i64 Start_index
    4:i64 End_index
    5:i8 Size
    6:string Err
}
//像broker注册自己并创建这个消费者对应的客户端，就可以像消费者发送pingpong和pub了
struct infoRequest{
    1:string ip_port
}
struct infoResponse{
    1:bool ret
}
//cli_Name来从broker中拿topic_oartition下面从index的数据以opyion的方式
struct InfoGetRequest{
			1:string  cli_Name  
			2:string  topic_Name 
			3:string  partition_Name 
			4:i64     offset 
            5:i8     option
		}
struct InfoGetResponse{
    1:bool ret
}

//-----------------------新加bro-bro，主broker先是让他准备好接收，然后告诉从节点具体接收文件的位置
struct PrepareAcceptRequest{
    1:string topic_Name
    2:string partition_Name
    3:string file_Name
}
struct PrepareAcceptResponse{
    1:bool ret
    2:string err
}

struct PrepareSendRequest{
    1:string topic_Name
    2:string partition_Name
    3:string file_Name
    4:i64 offset
    5:i8 option
}
struct PrepareSendResponse{
    1:bool ret
    2:string err
}
//服务器接口
//在thrift中service相当于给operation_server定义了一个接口
// push	处理生产者发来的推送消息
// pull	处理消费者拉取消息请求
// info	处理客户端状态报告、连接信息等
service Server_Operations{
    //producer
    PushResponse push(1:PushRequest req)
    //consumer
    PullResponse pull(1:PullRequest req)
    infoResponse info(1:infoRequest req)
    InfoGetResponse StarttoGet(1:InfoGetRequest req)
    //上面这些是消费者和客户端根broker交流，下面的是broker和broker之间的交流
    //1:通知目标 Broker：准备接收某文件
    PrepareAcceptResponse prepareAccept(1:PrepareAcceptRequest req)
    //2:通知接收方“我要从 offset 开始，发送某个文件的某部分了”，请确认你准备好了，或者已经收到了这部分
    PrepareSendResponse prepareSend(1:PrepareSendRequest req)    

}
//PushRequest 是客户端→服务端，用于写入消息到队列。
//PubRequest 是服务端→消费者客户端批量发送消息
//之前是i64 offset,现在变成了两个，有什么好处吗？？？
struct PubRequest{
    1:string topic_name
    2:string partition_name
    3:i64 start_index
    4:i64 end_index
    5:binary msg
}
struct PubResponse{
    1:bool ret
}
struct PingpongRequest{
    1:bool ping
}
struct PingpongResponse{
    1:bool pong
}
service Client_Operations{
    PubResponse pub(1:PubRequest req)
    PingpongResponse pingpong(1:PingpongRequest req)
}
//为啥他们长的不一样，上面那个发送只能给一个broker,但是消费的话这个分区可能有多个副本
//生产者获取broker的host和port
struct ProGetBroRequest{
    1:string topic_name
    2:string partition_name
}
struct ProGetBroResponse{
    1:bool ret
    2:string bro_host_port
}
//生产者---设置或更改某个特定分区的状态。
struct ProSetPartStateRequest{
    1:string topic
    2:string partition
    3:i8 option
    //副本数量
    4:i8 dupnum
}
struct ProSetPartStateResponse{
    1:bool ret
    2:string err
}
//消费者想消费某个 topic 的某个 partition，于是去询问哪个 broker 负责它。
struct ConGetBroRequest{
    1:string topic_name
    2:string partition_name
    3:i8 option
    4:string  cli_name
    5:i64    index
}
struct ConGetBroResponse{
    1:bool ret
    2:i64 size
   // 3:binary bros
    3:binary parts
}

//消费者现在要订阅一个topic下面的一个分区，那你肯定也要知道订阅模式
struct SubRequest{
    1:string consumer
    2:string topic
    3:string key//这里为什么要用key，感觉用分区就行了
    4:i8 option
}
struct SubResponse{
    1:bool ret
}
//在消费者消费消息的时候需要更新分区
// struct UpdatePTPOffsetRequest{
//     1:string topic
//     2:string part
//     3:i64 offset
// }
// struct UpdatePTPOffsetResponse{
//     1:bool ret
// }
//bro注册自己
struct BroInfoRequest{
    1:string bro_name
    2:string bro_host_port
}
struct BroInfoResponse{
    1:bool ret
}

struct BroGetAssignRequest{
    1:binary brokerpower
}
struct BroGetAssignResponse{
    1:bool ret
    2:binary assignment
}
struct UpdateOffsetRequest{
    1:string topic
    2:string part
    3:i64 offset
}
struct UpdateOffsetResponse{
    1:bool ret
}
struct UpdateDupRequest{
    1:string topic
    2:string part
    3:string brokerName
    4:string blockName
    5:i64 EndIndex
    6:bool leader
}
struct UpdateDupResponse{
    1:bool ret
}
struct ConStartGetBroRequest{
    1:string topic
    2:string part
    3:i64 offset
    4:i8 option
    5:string cli_name
}
struct ConStartGetBroResponse{
    1:bool ret
    2:i64 size
    3:binary parts
}
struct CreateTopicRequest{
    1:string topic_name
}
struct CreateTopicResponse{
    1:bool ret
    2:string err
}
struct CreatePartitionRequest{
    1:string topic_name
    2:string partition_name
}
struct CreatePartitionResponse{
    1:bool ret
    2:string err
}
service ZKServer_Operations{
    //producer
    ProGetBroResponse ProGetBro(1:ProGetBroRequest req)
    ProSetPartStateResponse ProSetPartState(1:ProSetPartStateRequest req)
    //consumer
    ConGetBroResponse ConGetBro(1:ConGetBroRequest req)
    SubResponse Sub(1:SubRequest req)
    //UpdatePTPOffsetResponse UpdatePTPOffset(1:UpdatePTPOffsetRequest req)
     //broker
    BroInfoResponse  BroInfo(1:BroInfoRequest req)
    BroGetAssignResponse BroGetAssign(1:BroGetAssignRequest req)
    UpdateOffsetResponse UpdateOffset(1:UpdateOffsetRequest req)
    UpdateDupResponse UpdateDup(1:UpdateDupRequest req)
    ConStartGetBroResponse ConStartGetBro(1:ConStartGetBroRequest req)
    //------------------
    CreateTopicResponse CreateTopic(1:CreateTopicRequest req)
    CreatePartitionResponse CreatePartition(1:CreatePartitionRequest req)

}


