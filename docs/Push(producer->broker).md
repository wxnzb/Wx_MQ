## push(也就是broker)
producer->broker
传进来的信息：存入in[Info]中
producer
topic,part
message,size
ack
cmdindex
内部处理Handle:
首先必须存在这个topic,要是不存在，直接返回就行
根据ack,分为两个
1：ack=-1:调用server里面的*parts_raft的Append接口，将in传入
2：ack=1||ack=0;前者同步，后者用go 异步；调用topic[*Topic]的addMessage接口,将in传入
1：*parts_raft:Append
首先在*parts_raft结构体下的Partitions成员里找part+partition[string]对应的Raft
并调用Raft的GetState接口看他是否是leader，要是不是直接返回
然后找到*parts_raft结构体下面applyindex成员里part+partition[string]的值是否等于0,要是等于0,说明当前分区快照还没有应用，直接返回
然后找到*parts_raft结构体下面CMD成员里找part+partition[string]+producer[string]对应的值，要是等于cmdindex,直接返回
构造OP[raft下面的]：
producer
topic,part
message,size
cmdindex
broker_index:*parts_raft下面的me成员,这是在rpc.go里面make初始化的时候就设置了
Operation:"Append"
Tpart:topic+part
然后找到*parts_raft结构体下面的CSM成员里找part+partition[string]+producer[string]对应的值，要是的变革与cmdindex,说明应将提交过了，在检查一下是否是leader就行；要是不等于，那么就先将找到的CSM里的lastindex存起来，这是为了要是没成功需要回退回去；
调用*parts_raft结构体下面的Partitions成员找part+partition[string]对应的Raft
并调用Raft的Start()接口
进入for循环：看*parts_raft下面的Add[chan COMD]成员是否接收到信息，要是在规定时间内接收到了，要是接收到的Add里面的index和前面发送Start返回的index像等，就直接返回；要是超时了，则判断是否还是leader,要是不是，就需要回退到lastindex并退出
分支：Start函数：参数：OP，beleader,leader
首先判断Raft是否被杀，要是被杀，就直接返回
要是没被杀
判断一下是否是leader,要是是，就构造LogNode,成员有：
Log:Op
Beleader:beleader
leader
Logterm:Raft下的currentTerm//这个是什么时候？
LogIndex:Raft下面len(log)+Raft下面的X//还有这个
将LogNode加入Raft的log中
将Raft的matchIndex++
开启携程Raft的接口：persist()
设置Raft的=0
开启携程Raft的接口appendentries(Raft.currentTerm)
分支：persist()
填充Per结构体:
X:Raft的X
Log:Raft的Log
Term:Raft的currentTerm//这个是什么时候的？？
VotedFor:Raft的votedFor//这个是什么时候的
将Per结构体进行Encode序列化
调用Raft的Persister结构体下面的SaveStateAndSnapshot接口
Persister结构体：
raftstate []byte
snapshot []byte 
分支：SaveStateAndSnapshot，参数：Per转化的[]byte，Raft的snapshot[]byte
将Per和snapshot传进Raft的Persister结构体进行保存
分支：appendentries,参数Raft的currentTerm
作用：Raft 协议中 Leader 向所有 Follower 发送 AppendEntries RPC，太长了，要卡自己去看
2：Topic的addMessage接口，参数Info
先判断在Topic下面的Parts[in.partition]可以找到Partition结构体不，要是找不到，就创建
然后调用Partition的AddMessage接口
分支：addMessage，参数：Info
先判断Partition的状态要是DOWN就直接返回
Partition下面的index+1
构造Message结构体：
topic
partition
Msg
Size
上面的Info中就有
Index：p.Index
然后将Message加入到Partition下面的queue里面
比较Partition中的index-Partition中的start_index大小，要是>=VERTUAL_10，寿命在队列里面已经存了10条消息了，现在要将他刷到磁盘，把Partition的queue的VERTUAL_10条取出来，然后噢欧进行json序列化，获取json序列化的大小size
构造Node结构体：
Start_index:p.start_index
End_index:
Size
将Node写入文件：通过：调用Partition下面的*File的WriteFile函数

分支：WriteFile函数，参数：Partition的fd,构造的Node,以及构造的Message

调用fd[os *File]的Write函数，将Node和Message存进去

更新start_index和queue,也就是把写入磁盘的去掉

调用in的zkclient的rpc函数UpdateDup,需要参数

topic,partition

BrokerName:in.BrokerName

BlockName:调用GetBlockName(in.file_name)得到

EndIndex:Partition下面的index

这个函数是broker->zkserver

内部调用了UpdateDupHandle，参数Info

也就是把上面的放在了Info中

Info里的

topic,partition

cli_name

blockname

index

首先构造str:也就是zkserver下的zk结构体里面的TopicRoot+...+blockname

调用zkserevr下面的zk的GetBlockNode接口，将str传进去从而从到blocknode结构体，长这样

blocknode:

Name

topic,partition

filename

startindex,endindex

leaderbroker

进行判断要是Info,index>blocknode的endoffset,就将他的值赋给blocknode的endoffset,然后调用RegisterNode函数进行更新数据

然后调用zkserver的zk下面的GetDuplicateNode,参数是str+cli_name也就是brokername获取DupNode结构体，他里面有

Name

topic,partition

blockname

startindex,endindex

brokername

也是将info的index赋给dupnode的endoffset并调用zkserevr的xk的RegisterNode接口进行更新