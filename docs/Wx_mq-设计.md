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
为每一个分区创建一个文件，采取顺序读写的方式提高性能
