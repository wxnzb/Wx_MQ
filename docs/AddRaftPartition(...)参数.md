AddRaftPartition(...)参数:

topic,part

Brokers ([]byte)

首先将Brokers通过json.Unmarshal解析到BrokerS,这个结构体有下面几个成员:

   BroBrokers map[string]string `json:"brobrokers"`

​    RafBrokers map[string]string `json:"rafbrokers"`

​    Me_Brokers map[string]int    `json:"mebrokers"`

然后内部调用AddRaftPartitionHHandle函数,传入参数:Info:

topic,part

brokers:BrokerS.RaftBrokers

brokerMe:BrokerS.MeBrokers

首先定义nodes(map[int]string),然后遍历Info的brokerMe,将每个int和string的对应关系存入到nodes中,初始化index:=0,那么他肯定会进入for循环index<len(Info的brokers),在for循环内部,首先找 *server的broker中nodes的index对应的string对应的bro_cli,这不就是根据brokerMe的string在brokers里面找到bro_cli吗???要是没找到,那么就调用raft_operations的NewClient创建bro_cli,传入参数是nodes的index对应的string,然后将他存入,然后把这个bro_cli加入到peers里面([]*raft_operations.Client),然后index++直到退出for循环.然后调用  *server的 parts_raft的AddPart_raft函数,传入参数:peers,*server的me,Info的Topic和part,  *serevr的aplych

分支:*server的 parts_raft的AddPart_raft函数,传入参数:peers,*server的me,Info的Topic和part,  *serevr的aplych

首先构建str:=topic+part,然后在 *parts_raft中的Partitions中找str是否存在,要是不存在,那么就调用raft的Make函数,把上面的参数又是全部传入,再传入一个&raft的Persister{}结构体,并将str和得到的 *raft的Raft加入parts_raft的Partitions中

分支:Make函数,把上面的参数又是全部传入,再传入一个&raft的Persister{}结构体....

在这个函数内部,首先他会构造raft然后就是创建携程跑 *raft的Commited函数,然后调用 *raft的readPresist阿訇所农户,在创建 *raft的ticker函数,这些你到时候补充把