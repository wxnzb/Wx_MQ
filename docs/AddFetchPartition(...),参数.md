AddFetchPartition(...),参数

type BrokerS struct {

​    BroBrokers map[string]string 

​    RafBrokers map[string]string 

​    Me_Brokers map[string]int    

}

首先将req.Brikers通过json.Unmarshal解析成BrokerS结构体,然后调用AddFetchPartitionHandle函数

AddFetchPartitionHandle函数,传入参数Info

topic,part

LeaderBroker

HostPort

brokers:BrokerS.BroBrokers

filename

首先调用 *Server的PrepareAcceptHandle函数,传入参数Info,然后判断Info的LeaderBroker要是== *Server的Name,也就是这个broker就是leaderBroker,1:那么进入分支,判断 *Server的topics中的Inf.topic是否存在对应的*Topic,要是不存在就直接返回,然后遍历Info的brokers中的brokername,在for循环中调用*Topic的PrepareSendHandle函数,传入参数Info:

topic,part

consumer:brokername

filename

option:TOPIC_KET_PSB_PULL

2:要是不相等,那么首先在 *Server的brokers_fetch中找Info的leaderBroker对应的 * server_operations.Client,要是找不到,就调用server_operations的NewClient函数,传入参数Info的HostPort,然后把Info的LeaderBroker和bro_cli的对应关系加入 *server的brokers_fetch,然后构造str:=Info的topic+part+file_name,然后查看 *server的parts_fetch中str是否存在,要是不存在,就将str和Info的LeaderBroker的关系加入到 *server的fetch_fetch中,然后找 *server的topics的Info的topic对应的 *Topic,然后调用 *server的FetchMsg函数,传入参数,Info,bro_cli,topic

1:

分支:*Server的PrepareAcceptHandle函数,传入参数Info

首先查看 *server的topics的Info的topic对应的 *Topic是否存在,要是不存在就创建并把他们的关系存如 *server的topics里面,然后调用 *Topic的PrepareAcceptHandle函数,传入参数,Info

分支: *Topic的PrepareAcceptHandle函数,传入参数,Info

调用os的Getwd函数返回str,然后str+= *Topic的Broker+Info的topic+...+file_name,然后调用NewFile,传入str,返回 *File和fd,然后将str和 *File的关系存入 *Topic的Files中,在 *Topic的parts里面找Info的part对应的 *Partition,然后调用 *Partition的StartGetMessage函数,传入参数 *Fiole,fd,Info

分支: *Partition的StartGetMessage函数,传入参数 *Fiole,fd,Info

在这个函数中,首先判断 *Partition的state,要是已经是ALIVE状态,将返回ErrHadStart,要是关闭,那么对 *Partition进行一系列的设计:

state

file

fd

file

index:*File的GetIndex函数,传入参数fd,找到最后一条消息索引

start_index:等于上面的index,哪为什么要相等

------------------------------整体上上面就是给 *Partiton赋值一系列消息

分支: *Topic的PrepareSendHandle函数,传入参数:Info:

topic,part

consumer:brokername

filename

option:TOPIC_KET_PSB_PULL

首先调用os的Getwd函数得到str,str+= *Topic的Broker+Info的topic+...+filename,然后再 *Topic的Files中找str对应的 *File,包那哪个找到呀,上面都进行处理了,然后要是找不到,调用CheckFile进行创建,那我感觉这个和上面的NewFile不就是一样的马,调用GEtStringfromSub函数,传入Info的Topic,part和option,然后在 *Topic里面找 sublist的sub对应的 *Subscription是否存在,要是不存在,就调用 NewSubscription函数,传入参数Info,sub_name,*Topic的parts和Files进行创建,然后根据Info的option进行选择,这个的话一定是TOPIC_KEY_PSB,那么就会调用 *subscription的AddNode函数,传入参数Info和 *file

分支: *Subscription的AddNode函数,,传入参数Info和 *file

构建str:=Info的topic+...+consumer,这个consumsre是什么传进去的呢???然后再 *Subscription的nodes中找str对应的 *Node,要是找不到,就调用NewNode函数,传入参数Info和 *File,这个就完事了

虽然这个超级长,但是你一定可以完成任务的,加油,你是最棒的

2: *server的FetchMsg函数,传入参数,Info,bro_cli, *Topic

首先调用 *Topic的GetFile,传入参数Info得到 *File,fd,然后调用 *File的GetIndex函数,传入参数fd得到当前文件的end_index,然后进行判断,要是

2.1:Info的filename!="Nowfile.txt"时,创建go携程,在携程里面,首先调用NewPartition函数,传入参数 *server的Name和Info的topic和part得到 *Partition,然后调用 *partition的StaerGetMessage函数,传入参数 *File,fd,Info,这个函数上面也讲了,ice:=0然后在for循环中,调用 bro_cli的Pull这个rpc拉取消息,他 传入的参数是:

consumer: *server的name

topic,part

offset:end_index

size:10

option:TOPIC_KEY_PSB_PULL
要是拉取成功,那么ice:=0又重新设置成0,然后构造DataNode结构体:start_index和end_index和size,都来自pull的resp,然后调用 *File的WriteDile,传入参数fd,DataNode,resps.MSGS,index=resp.EndIndex+!.然后调用 *server的zkclient的UpdateDup这个rpc函数,传入参数:

topic,part

brokername: *server.Name

BlockName:GetBlockNode(Info.name)

EndIndex:resp.EndIndex

要是拉取失败,那么ice++,要是一直拉取都是失败,那么尝试3编后,调用 *server的zkclient的GetNewLeader函数,传入参数:topic,part,blockname,也就是filename[:len(Info.file)-4],然后在 *server的brokers_fetch中找Info的topic+part是否存在,要数不存在就调用server_operations的NewClient函数,传入参数resp.Hostport也就是得到了leader_bro,然后将他和resp.LeaderBroker对应关系存入 *server的brokers_fetch中

2.2:要是 Info的filename=="Nowfile.txt"  ,这个就少了上面的哪一步StartGetMessage函数,然后就是进入for循环,调用bro_cli的pull函数 ,要是调用成功,但是resp的Size==0,那么就进行等待然后继续for循环,要是不是,那么证明拉取道数据了,直接将得到的resp.MSGS Unmarshal成 []Message,然后遍历[]Message,在for循环中,调用 *Topic的addMessage函数,传入参数Info

topic,part

BrokerName: *server的Name

zkclient: *server的zkclient

file_name:"NowBlock.txt"

message:Message的Msg

size:Message的Size

上面这个是成功的,那么ice需要重新设置城0.要是拉取不成功,那么在拉取失败次数大于3时,和上面时一样的,就是在调用 *server的zkclient的GetNewLeader函数,传入参数:topic,part,blockname是,blockname他是"NowBlock.txt"                                                                     