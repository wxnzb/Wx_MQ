### CreateTopic(?->zkserver)

成员：

topic

内部调用rpcserver的zkserver的CraeteTopicHandle函数，参数Info_in

topic

分支：CreateTopicHandle函数

创建zookeeper的TopicNode结构体，内部

name:Info_in的Topic

然后调用 *zkserver的zk结构体的RegisterNode函数，将TopicNode结构体传入

分支:zk结构体的RegisterNode函数

通过构建path:=zk的TopicRoot+"/"+TopicNode的name，判断这个path是否存在，要是存在，那么就将TopicNode设置进去，但是我感觉这步没什么用，要是不存在就进行创建，然后进入i.Name()=="TopicName"进入这个分支，part的path:=path+"/"+Partitions,通过Exists来判断是否存在，要是 存在就直接返回，要是不存在就进行创建，然后sub的path:=path+"/"+"Subscription"跟上面一样，先判断是否存在，要是存在就直接返回，要是不存在就进行创建

这个还挺简单的，就是创建三个路径，topic,partition,subscription的，这里你最需要明白的是Create函数他主要是创建了什么，文件吗？？