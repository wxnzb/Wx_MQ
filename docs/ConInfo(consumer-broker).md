### ConInfo(consumer->broker)

这个rpc主要是用来向broker注册消费者自己，从而使得broekr可以定时向消费者发送心跳看消费者还活好着没

需要成员：Consumer的IpPort

调用rpcserevr的server下面的InfoHandle接口，参数IpPort

分支：InfoHandle

首先根据IpPort调用的NewClient函数获得了这个consumer作为服务端是对应的客户端，然后将这个加入到server的consumers中，他先是通过IpPort在server的consumers中找 *Client，要是找不到就通过NewClient,参数IpPort和client创建一个*Client并添加到server的consumers中，然后开起一个携程，调用了server的checkConsumer,将*Client传进去，就完事了

分支：NewClient,参数IpPort和client来创建*Consumer

Client结构体长这样：

name:IpPort

consumer:client

state:ALIVE

subList

分支：checkConsumer

首先调用*Client的CheckConsumer接口，返回shutdown[bool]看是否消费者一经关闭连接，要是关闭连接，就循环遍历*  *Client中的subList中对应的 *Subscribtion,并调用* *SubScription的ShutdownConsumerInGroup函数，参数* *Client的name也就是IpPort

果然：

分支：*Client的rpc接口CheckConsummer

调用的*Client的consumer的Pingpong要是接收不到就退出，并将* *Client的state设置成DOWN

分支：*SubScription的ShutdownConsumerInGroup,参数是IpPort

首先获得这个*SubScription的option，要是他是TOPIC_NIL_PTP_PUSH,那么就直接在*Scription的groups中的[0]中也就是第一个*Grooup中调用DownClient接口，要是option是TOPIC_KEY_PSB_PUSH,那么就需要遍历* *SubScription中的cgroups,然后将每个 *Group中都调用DownClient接口

分支：*Group的DownClient函数，参数;IpPort

将*Group的consumers对应的IpPort设置成false