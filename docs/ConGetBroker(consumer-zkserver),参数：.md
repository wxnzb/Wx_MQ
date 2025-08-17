### ConGetBroker(consumer->zkserver),参数：

topic,part

option

index

cli_name

内部调用rpcserver的zkserver的conGetBrokerHandle函数，参数Info_in

topic,part

option

index

cli_name

首先调用zkserver的zk的CheckSub,传入参数Info_in，内容和上面一样，然后判断info的option要是是TOPIC_PTP,那么调用zkserver的zk的GetBrokers,要是他是TOPIC_PSB,那么调用zkserver的zk的GetBroker，传入参数Info的topic和part和index，他们都返回了Parts([]Part),然后调用zkserver的SendPreoare,传入参数Parts和Info_in,他里面主要是把上面得到的beoker给他发消息说准备发送把，返回[]client.partkeys,然后json partkeys,直接返回

分支：zk的CheckSub函数，参数Info_in,这个还没写？？？

分支:zk的GetBrokers,传入参数：Info_in的topic,目的是获取某个topic的所有broker信息

首先构造path:=zk的TopicRoot+...+"Partition",然后调用zk的Con的Children，将path传入得到这个topic下面的所有partitions([]string),然后遍历[]string,在循环内部，调用zk的GetPartitionNode,传入参数path+part得到PartitionNode,然后将PartitionNode的PTPoffset赋给PTP_index,然后调用zk的Con的Children函数，传入参数path+part,得到了这个partition下的所有blocks([]string),然后遍历blocks,再循环中，调用zk的GetBlockNode,传入参数path+part+block得到BlockNode,然后进行判断Blocknode的StartOffset<=PTP_index&&BlockNode的EndOffset>=PTP_index,也就是找到consumer需要的index在这个topic的所有partitions中的特定block,要是可以，那么进入if,是调用zk的Con的Children，传入参数：path+part+BlockNode.Name,得到这个block的所有副本Duplicates([]string),然后进行遍历,在for循环里面，调用zk的GetDuplicatenode函数，传入参数path+part+BlockNode.Name+duplicate,得到DuplicateNode,之前初始化了一个max_dup(DuplicateNode),并将他的EndOffset初始化为0,现在要是max_dup的EndOffset==0||max_dup.EndOffset<=duplicateNode.EndOffset,那么调用zk的ChexkBroker传入参数duplicateNode的BrokerName,看Broker是否在线，要是在线，那么max_dup=duplicatenode,也就是找到所有副本中endoffset最大的,然后调用zk的GetBrokerNAme，传入参数max_dup的brokername得到BrokerNode,之前定义了parts([]Part),现在加入他

Part:

topic,part

BrokerName:BrokerNode.Name

BroHost_Port::BrokerNode.BroHostPort

RaftHost::BrokerNode.RaftHostPort

PTP_index:PTP_index

File_name:BlockNode.fileName

最后返回Parts

分支:zk的GetBroker,传入参数：Info_in的topic,part和index

和上面的基本长的一样，唯一的区别是他直接少了一层遍历partitions的for循环，其他都一样,因为他传入了一个index，那么不是找最大的，而是包含index的

分支:zkserver的SendPreoare函数，传入参数[]Part,Info_in

首先遍历[]Part,在for循环中，首先判断part的Err是否等于Ok,要是不等于，说明上面得到的根本就没在线，将他加入到[]clients.PartKey中，PartKey:ErrLpart.Err,直接跳过，要是Ok,那么找zkserver的brokers的part.NrokerName对应的bro_cli,要是没找到，就调用server_operations的NewClient,传入part.BroHost_Port创建bro_cli,并把BrokerName和bro_cli的对应关系添加到zkserver的brokers，然后判断Info-in的option，要是是TOPIC_PTP,那么就给i赋值为PTP_index,要是是TOPIC_PSB,那么就给i赋值Info_in的index,然后调用bro_cli的PrepareSend这个rpc函数，传入参数

topic,part

option

consumer:Info_in的cli_name

FileName:part的FileNAme

然后调用append,添加clients.PartKey

name:part

Brokername

broker_H_P

Err

最后返回[]clients.PartKey