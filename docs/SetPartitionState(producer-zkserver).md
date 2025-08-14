###  SetPartitionState(producer->zkserver)

成员：

topic,partition

option

dupnum

内部调用rpcserver的zkserver的SetPartitionStateHandle函数，参数Info_in

topic,partition

option

dupnum

首先调用zkserver的zk的GetPartState函数，传入参数Info_in的topic和Info_in的part获得PartitionNode结构体，Info_in的option和PartitionNode的option做比较，要是不一样，说明option有变化，那么首先调用zkserver的zk的GetPartBlockIndex函数，参数Info_in的topic和part,目的是获得PartitionNode的Index,然后调用zkserver的zk的UpdatePartitionNode,传入参数zookeeper的PartitionNode:

topic,partition

option

dupnum

index

PTPOffset:PartitionNode的PTPOffset

然后就将这个路径下的内容更新成新的，我还感觉挺奇怪的，那不就是事实上更新了一个option吗，为什么要搞的这么复杂？1:是PartitionNode的option==-2，1.1然后要是Info_in的option==-1:那么首先调用zkserver的GetDupsFromConsist,参数是Info_in，返回的第一个是给这个topic+partition找的三个brokername的duplicateNode,第二个是brokername和host呀之间的关系，然后调用zkserver的BecomLeader函数，传入参数Info_in:

topic,partition

cli_name:zookeeper.duplicateNode的第一个[0].Brokername

这个函数主要是根据path:zkserver的zk的TopicRoot+...+part+"NowBlock"找到这个路将下的NowBlock并对他进行更新，主要是更新了NowBlock.LeaderBroker==Info_in.cli_name,也就是将第一个得到的brokername赋给他了，然后遍历上面得到的[]zookeeper.DuplicateNode,在循环中：首先在zkserver的Brokers中找DuplicateNode.BrokerName对应的bro_cli,要是找到了，调用bro_cli的rpc：AddRaftPartition函数，传入参数：

topic,partition

Brokers:上面GetDupsFromConsist的第二个参数，就是得到的BrokerS的json值

1.2然后就是default分支：首先调用zkserver的GetDupsFromConsist,参数是Info_in,这个和上一个option==-1的开始操作是一样的，然后调用zkserver的zk的GetBrokerNode,传入参数上面函数返回的第一个参数[]zookeeper.DuplicateNode的第一个的BrokerName,获得leaderBroker(BrokerNode),然后遍历[]zookeeper.DuplicateNode,跟上面一样，找到zkserver的Brokers中DuplicateNode的BrokerName对应的bro_cli,并调用bro_cli的AddFetchPartition,参数是：

topic,partition

Brokers

LeaderBroker:上面的LeaderBroker.name

HostPort:上面的LeaderBroker的BrokHostPort

FileName:"NowBlock.txt"

2:要是PartitionNode的option不是2,那么就不进入上面的分支，首先调用zkserver的zk的GetPartNowBrokerNode函数，参数是Info_in的topic和partition

分支：zk的GetPartState参数Info_in的topic和part,他构造path:=zk的TopicRoot+"/"+topic+"/"+Partitions+"/"+partition,先判断是否存在，要是存在，在调用zk的*zk.Con的Get(path),获得这个path里面的信息，并json解析放入PartitioNode结构体中

分支：zk的GetPartBlockIndex函数，参数Info_in的topic和part,他构造path:=zk的TopicRoot+"/"+topic+"/"+Partitionns+"/"+partition,然后调用zk的GetPartitionNode(path),和上面是一样的呀，判断是否存在，要是存在就获得PartitionNode,然后返回PartitionNode的Index,

这两个这么向，为什么上面哪个不调用GetPartitionNode函数呢，真是奇怪

分支：zkserver的GetDupsFromConsistent,参数是Info_in,

首先构造str:=Info_in的topic和partition,调用zkserver的*Consistent的GetNode函数，参数是str+"dup",3,他返回的是根据str+dup得到的对应的hash后面的三个的brokername;然后构造了3个zookeeper的DuplicateNode:

topic,partition

startoffset:0

brokername:GetNode返回的[0]  [1] [2]

BlockNAme:"NowBlock"

name:dup_0/1/2,

然后将他统一加入到[]zookeeper.DuplicateNode中，然后遍历这个数组，每个都调用zkserver的zk的RegisterNode,这个函数里面就是先构造str:=zk的TopicRoot+"/"+DuplicateNode的topic...的blockname的name,然后在这个路径将DuplicateNodejson后的添加进去，然后定义一个BrokerS,用到这个结构体这些东西：

BroBrokers:map[string]string

RaftBrokers:map[string]string

Me_Brokers:map[string]int

然后循环[]zookeeper.DuplicateNode,在循环体内部，首先调用：zkserver的zk的GetBrokerNode,传入参数DuplicateNode的BrokerName获得BrokerNode,并给BrokerS的那三个分别赋值，...[DuplicateNode的BrokerName]=BrokerName.BroHostPort/RaftHostPort/Me.这个函数的主要目的就是给这个topic+part_dup找到3个brokerName,并构造成[]zookeeper.DuplicateNode返回，然后将BrokerS进行json解析返回

分支:*Consistent的GetNode函数，参=参数是topic+partition+"dup",3,他内部调用了 *Consistent的SetBroHFalse函数,他就是遍历  *Consistent的BroH([string]int),然后将他初始化为false,然后调用*Consistent的hashkey函数，参数是上面第一个参数topic+partition+dup,获得int32,然后for循环第二个参数，也就是三次，在for循环里面，调用 *Consistent的getPosition，参数int32,就是上面得到的哪个hash值，通过getPosition得到第一个大于hash值的数int,然后通过 *Consistent的circle[c.hashSortedNodes[i]]找到对应的broker_name,然后吧内次遍历得到的broker_name放在一个[]string里面，并将 *Consistent的BroH的[broker_name]对应的设置成true,最终返回brokers_name哪个[]string

分支：