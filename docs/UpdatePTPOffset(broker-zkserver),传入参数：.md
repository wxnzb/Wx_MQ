### UpdatePTPOffset(broker->zkserver),传入参数：

topic,part

index

内部调用UpdatePTPOffset,传入参数Info_in:

topic,part

index

首先构造str:=zkserver的zk的TopicRoot+...+part，然后调用zkserver的zk的GetPartitionNode掺入参数str应该是可以得到PartitionNode的，要是不存在就直接返回，然后调用zkserver的zk的UpdatePartitionNode函数，传入参数PartitionNode:

topic,part

index

对这个PartitionNode进行更新