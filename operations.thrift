namespace go api
// 推送消息
//下面的key是分区
struct PushRequest{
    1:i64 producerId
    2:string topic
    3:string key
    4:string message
}
//推送消息是否成功
struct PushResponse{
    1:bool ret
}
//拉取消息所需的信息
struct PullRequest{
    1:i64 consumerId
    2:string topic
    3:string key
}
//拉取到的消息
struct PullResponse{
    1:string message
}
//这个先在还不太清楚干啥
struct infoRequest{
    1:string ip_port
}
struct infoResponse{
    1:bool ret
}
//服务器接口
//在thrift中service相当于给operation_server定义了一个接口
// push	处理生产者发来的推送消息
// pull	处理消费者拉取消息请求
// info	处理客户端状态报告、连接信息等

service Server_Operations{
    PushResponse push(1:PushRequest req)
    PullResponse pull(1:PullRequest req)
    infoResponse info(1:infoRequest req)
}
//PushRequest 是客户端→服务端，用于写入消息到队列。
//PubRequest 是服务端→客户端，用于回调、下发、通知、甚至测试心跳机制。
struct PubRequest{
    1:string msg
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
    PingpongResponse pingpong(1:PingpongRequest req)
}
struct SubscribeRequest{
    1:int
}
