## 基础架构
网络库+消息队列+数据持久化+zookeeper
### 消息队列
- 生产者生产消息
- 消费者消费消息(消费消息后不会被删除，后期提供日志检测功能)
- 消息队列持久化
- 通过zookeeper管理分片
- 通过Producer像broker push消息并由Consumer从broker pull 消息
#### Broker
单个消息队列服务器，支持水平扩展
#### topic
每个broker拥有多个topic,topic是不同消息的分类
#### Partition
将topic分成分成一个或多个Partition，消息的kv中按照k进行分区，不同的分区可能在不同的集群，消费者想知道自己需要的k的value首先就需要是在哪个集群【这里可以用zookeeper来做服务注册和发现】，然后又k找到需要的v
#### 高可用
为了提高系统的高可用性，需要对数据进行备份，将一份数据备份到多台及其，构成一个集群，这样一个broker出了故障可以立即用这个集群上面的其他broker代替
### 网络库
通过网络库进行进程间通信-rpc
#### rpc
rpc只支持单向调用，导致mq只支持push(生产者将消息发送至broker)和pull(消费者主动从broker拉取消息)，想要实现broker主动将消息推送给消费者，就需要双向rpc服务，所以在client也要注册服务
### 数据持久化
### SubScription
消费模式支持两种：点对点（point to point）和订阅发布(sub and pub)；每个Topic都会可能会有这两种模式，所以每个Topic将拥有两个SubScription，我们会将这个范围扩大，让每个Partition拥有两个SubScription，分别支持这两种方式。当有消费者要订阅的情况分别如下：

point to point：SubScription中只能有一个消费者组，Topic中的一条消息只能被一个消费者消费。我们会将这个范围扩大，每个topic中的一个partition只能被一个消费者消费。当有消费者选择这个模式时，将判断是否有一个group，若无则创建一个，若有则加入；

sub and pub : SubScription中可以有多个消费者组，每个消费者组中只有一个消费者。
简单来说点对点消费就是只能一个消费者组消费一个topic,但是发布订阅就是可以多个消费者组共同订阅一个，那么那么这个消息可能会以广播的形式发送给他的所有消费者
### Client
当consumer连接到MQ后，consumer会发送一个info,让MQ通过RPC连接到该消费者客户端，维护一个Client,该Client中保留可以发送Sub等请求的consumer,和该消费者客户端所有的订阅，这里采用map[string]*SubScription,和一个表示消费者客户端的状态
### 消费者组
consumer group下可以有一个或者多个consumer,consumer可以是一个进程，也可以是一个线程
```
为每一个分区创建一个文件，采取顺序读写的方式提高性能
//负责管理所有topics和连接的消费者客户端
type Server struct{
    topics map[string]*Topic
    consumers map[string]*Client
    mu sync.Mutex
}
//一个连接到服务器的消费者客户端
type Client struct{
    mu sync.RWMutex
    name string
    consumer client_operations_Client//消费者操作接口
    subList map[string]*SubScription//要是这个消费者关闭就要遍历这个列表，将消费者从中移除
    state string
	partition map[string]*Partition
}
//消费者组:一个消费者组只能订阅一个topic吗
type Group struct{
	rmu sync.RWMutex
    topic_name string
    consumers map[string]bool //map[client.name]alive
}
//消息主题，生产者将消息发送到主题，消费者从主题取消息
type Topic struct{
	sync.RWMutex
	Parts map[string]*Partition//该主题下的所有分区
	<!-- SubScription_Key_PSB map[string]*SubScription//带key的订阅，为了实现更复杂的订阅，比如只有消息的键匹配才发送给订阅者
	SubScription_NIL_PTP *SubScription//不带key的订阅 -->
    subList map[string]*SubScription//该主题下的所有订阅
}
//分区
type Partition struct{
	rmu sync.RWMutex
	key string
	queue []string//消息队列，存储实际内容
    consumers map[string]*Client//该分区下的所有消费者
	consumers_offset map[string]int//存储消费者对该分区消费的位置
}
//订阅结构体：消费者组如何从主题中获得消息的规则和状态
type SubScription struct {
	name               string //topicname+option类型
	rmu                sync.RWMutex
	topic_name         string
	consumer_partition map[string]string //一个消费者可以属于多个消费者组
	groups             []*Group
	option             int8
	consistent         *Consistent
}
type Consistent struct {
	rmu              sync.RWMutex
	hashSortNodes    []uint32          //排序的虚拟节点
	circleNodes      map[uint32]string //虚拟节点对应的世纪节点
	virtualNodeCount int               //虚拟节点数量
	nodes            map[string]bool   //已绑定的世纪节点为true，这个还不太了解
}
```
## Zookeeper
保存每个Broker的信息，和Topic-Partition的信息
## Producer
生产者通过Topic和Partition的name查询Zookeeper找到要发送的broker,通过push将消息发送给broker
生产者需要将链接Zookeeper茶锈拿到的信息进行存相互，要是下次有topic-partition的信息就不需要在查询了
若生产者发送给Zookeeper要查询的topic-partition信息不存在，生产者就新创建一个，???这里生产者创建一个什么？？？Zookeeper会通过负载均衡将这个topic-partiiton分给一个broker,broker受到一个自己之前没有的topic-partition会新创建一个
但是partition不能无限创建，先设置只能创建10个，超过数量就会创建失败
## consumer
消费者连接到zookeeper查询需要的topic-partition,并连接到broker
## 消费者首次连接
创建一个消费者客户端，设置他的状态是state，一般是alive;现在这个消费者要订阅某个分区，他会进行判断这个消费者订阅是否合理，要是合理，消费者客户端创建拉取线程，他是创建一个topic-partition队列，从存储读取一块信息到该队列，并维护一个Pingpong心跳
## 断开连接
broker通过Pingpong检测到消费者客户端时取消息后，修改消费者客户端的状态
还有一种情况，broker正常发送消息，但是消费者在一定时间内没有回应，总则将broker专门发送消息给这个消费者的这个携程移到超时队列，要是重新发送还是不行，就将他移动到死信队列，直到消费者发送resume，broker将这个携程从死信队列取出
### 举例说名
向 Zookeeper / Broker 注册：我想订阅 topic=order, partition=p-001

ZK 回复该 partition 属于 Broker-2

consumer-A 连接 Broker-2，发出订阅 + offset 请求

Broker 检查是否已被占用（P2P 限制一个消费者）

若合法：

建立通信通道

创建拉取线程，从 offset=1024 开始读取数据

启动心跳线程保持存活状态

