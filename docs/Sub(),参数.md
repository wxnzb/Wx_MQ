Sub(),参数:

consumer

topic,part

option

内部调用SubHandle函数,传入参数Info,传入成员也就是上面这些

首先在server中的topics中通过Info的topic找对应的*Topic,要是没找到就进行创建,也就是调用NewTopic,传入参数server的Name和Info的part,然后调用*Topic的AddSubscription函数,传入参数Info

分支:*Topic的AddSubscription函数,参数Info,

首先调用GetStringfromSub函数,传入参数Info的topic,part和option,然后在 *Topic的sublist中找ret对应的*Subscription,然后要是没找到,就进行创建,调用NewSubscription函数,传入参数Info,ret和*Topic的Files,然后将ret和 *Subscription这对应关系存如*Topic的sublist中,不然要是在  *Topic的Sublist中可以找到,直接调用   *Subscription的AddConsumer函数,传入参数Info

分支:NewSubScription函数,传入参数Infomret,*Topic的parts和files

他会创建一个Subscription结构体,然后调用NewGroup函数,传入参数Info的topic和consumer,返回*Group,这个函数里面就自动将consumer加入了,group.consumers[consumer] = true,然后将这个返回的*Group加入到 *SubScription的groups中

分支: *subscription函数的AddConsumerInGroup,传入参数Info

首先switch Info的option,要是是TOPIC_NIL_PTP_PUSH,那么就调用  *subscription的groups([] *Group)[0]的AddClient,传入参数,Info的consumer,要是等于TOPIC_KEY_PSB_PUSH,那么调用NewGroup函数,传入参数Info的topic和consumer,在调用append将这个 *Group加入到cgroups中

