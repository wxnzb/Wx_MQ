### PrepareState(zkservre->broker)参数

这个rpc函数首先将传入的req.Brokers反json化成BrokerS,然后调用rpcserver的server的PrepareStateHandle函数，传入参数:Info

topic,part

option:req.state

brokers:BrokerS的BroBrokers

在server的PrepareStateHandle函数函数内部，首先判断Info的option==-1时，调用server的parts_rafts的checkarttate函数，传入参数：

Info的topic和part

分支;*parts_raft的CheckPartState函数，传入参数Info的topic,part

首先构造str:=topic+part

然后在*parts_raft的Partition中找是否有str,返回bool的结果，所以他传入brokers的意义是什么？？？



