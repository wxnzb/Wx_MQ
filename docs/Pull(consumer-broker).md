### Pull(consumer->broker)
需要的参数
consumer
topic,partition
offset
size
option
内部调用rpcserver下面的server的PullHandle函数，将上面的传进参数Info中
consumer
topic,partition
offset
size
option
server的PullHandle函数
首先判断传的UpdatePTPOffset这个进来的option是不是TOPIC_NIL_PTP_NULL,要是是，就调用server的zkclient的UpdatePTPOffest函数，将
topic,partition
offset传进去
然后就开启了
broker->zkserver的UpdateOffset这个函数，他内部调用了rpcserevr的szkerver的UpdatePTPOffsetHandle接口，并将上面客户端传来的放入Info中
topic,partition
index
分支：UpdatePTPOffset函数
构造str:zkserver下面的zk的TopicRoot+..+partition,调用zkserver下面zk的GetPartitionNode(str)看这个node是否存在，要是不存在就直接退出
然后构造zookeeper的partiiton结构体：
topic,part
PTPoffset:info.index并将结构体传进zkserver的zk的UpdatePartitionNode函数中，也就是更新zk中这个partitionNode，就是有点好奇，为啥他的option是这个就要进行这个分支，到时候一探究竟把
然后这个分支就完了
然后从server中的topics中根据info中的topic获取对应的*topic,然后调用*topic的PullMeaage接口，将Info传进去
分支:PullMessage函数
先根据Info中的topic,partiiton,option调用GetStringfromSub获得sub_name,然后在*topic中的subList的sub_name找到*SubScription,然后调用*SubScription的pullmessage函数，将Info传进去
分支：pullMeaage
先获得node_name=topic+partiiton+consumer
在*SubScription的nodes中找node_name对应的*Node,调用*Node的ReadMSGS接口，将Info传入
分支：ReadMSGS
判断*Node的offset==-1或者*Node的start_index！=Info的offset，那么调用*Node的*File的FindOffset函数，参数是*Node的fd和Info的Offset,他主要是找到offset所在的node的偏移量，然后初始化nums:=0,要是nums<Info的size,那么调用*Node下的*File的ReadBytes函数,ReadBytes函数下面讲了，然后就给nums+node.SIze, *Node的offset+=NODE_SIZE+node.size,给MSGS赋值，

MSG结构体赋值：

size=nums

arry里面将上面获得的msg加入

start_index==刚开始第一个nums=0的时候的node.Start_index

End_index=node.EndIndex

这个函数还需要了，我有几个问题，首先consumer说要从offset开始获取size大小的消息，要是ooffsett刚好是在消息体中间那又要回到这个消息体对应的消息头的第一个开始了，然后就是他要获取size大小，但是体他是一块消息一块消息读的，要是步读最后一块消息就小于size，但是读了就有大于size那那不是读多了吗？？？

ReadBytes函数，参数：*os.File和offset

调用*os.File的ReadAt先从off读去NODE_SIZE字节，创建buf，将data_node写入buf,然后从Buf对数据存入node,offset+node.Size从*os.File调用ReadAt读取数据存到data_msg中返回node和data_msg

所以我不是很理解为什么需要这个步骤

​    binary.Write(buf, binary.BigEndian, data_node)

​    binary.Read(buf, binary.BigEndian, &node)

我感觉可能就是直接ReadAT获得的data_node[byte]每发直接转换成结构体

ok,完成！！！

加油，一切体验接回成为过去是，只有实力不会撒谎

