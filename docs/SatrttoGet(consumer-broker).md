### SatrttoGet(consumer->broker)

内部调用rpcserver的server下的StarttoGetHandle,传入参数：Info

Info结构体：

consumer

topic,partition

offset

分支：StarttoGetHandle

首先判断Info的option,但是太起卦了，他根本就没有传入Info的option啊！！

要是option是TOPIC_KEY_PSB_PUSH那么其实什么都不用做，要是option是TOPIC_NIL_PTP_PUSH,首先获得sub_name通过调用GetStringfromSub函数，然后需要调用server下面topics的Info的topicname对应的*Topic的HandleStartToGet函数

分支：*Topic的HandleStartToGet函数，传入参数，sub_name,Info,以及server的consumers的Info的consumer对应的 *Client的GetCli函数，为了最终获得可以发送给这个consumer服务端的客户端

首先他通过sub_name在*Topic的subList中找到对应的 *SubScription,然后调用*SubScription的AddConsumerInfog函数，将Info和cli[这个consumer的服务端对应的客户端]

1：分支：*SubScription的AddConsumerInConfig函数，参数Info和cli[这个consumer的服务端对应的客户端]

首先判断Info中的option，要是是TOPIC_NIL_PTP_PUSH,那么调用的server的PTP_config[ * Config]的Addcli接口，将Info的consumer和cli传入，要是optiion对应的是TOPIC_KEY_PSB_PUSH,那么首先在server的PSB_configs中找Info的partname+Info的consumer对应的*PSBConfig_PUSH,那么调用 *PSBConfig_PUSH的Start函数，将Info和cli传入

分支：*Config的AddCli函数，consumer和cli

首先*Config的cons_num++,然后 *Config的clis中天加上consumer和cli的对应关系，然后调用 *Config的consistent的Add函数，传入参数consumer和1,然后调用 *Config的RebalabcePtoC()和 *Config的UpdateParts()函数

分支：*Consistent的Add函数，参数consumer,1

首先得到*Consistent的nodes里找consumer，要是没找到就直接返回，给*Consistent的nodes的consumer对应的设置成true,然后将*Consistent的Con的consumer对应的设置成false，根据一个consumer对应的节数设置虚节点并对应上consumer，然后将这些加入到*Consistent的hashSortedNodes里面并进行排序

分支：*Config的RebalabcePtoC()和 *Config的UpdateParts()函数这两个先暂时不看了，嘿嘿不想看了

2：分支：*PSBConfig__PUSH的Start函数，参数：Info,cli

首先将*PSBConfig_PPUSH的Cli设置成cli,然后调用*PSBConfig_PUSH的*Part的UpdateClis函数和*PSBConfig_PUSH的*Part的start函数

分支：*Part的UpdateClis函数，参数names([]string类型，首先里面有了consumer),clis(map[string] *client,首先里面有了consumer->cli)

调用CheckChangeCli，参数*Part的clis(map[string] *client),names([]string)返回reduce和add([]string),对于reducce遍历后调用delete将reduce里买那的从* *Part的clis里面删除，对于dda遍历后首先在 *Part的clis俩面将对应add的nam对应的cli以其添加进去，然后调用携程 *Part的SendOneBlock函数

分支：*Part的SendOneBlock函数，参数add的每个consumer这个name还有它对应的*client,这个挺多的，到时候看

