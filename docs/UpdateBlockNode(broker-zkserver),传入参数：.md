### UpdateBlockNode(broker->zkserver),传入参数：

topic,part

cli_name:brokername

blockname

index:req.EndIndex

内部调用UpdateBlockNodeHandle。传入参数Info_in:也就是上面那些

首先构造str:=zks.zk.TopicRoot+...+Info_in的blockname，然后调用zkserver的zk的GetBlockNode，传入参数str得到BlockNode,进行if判断，要是Info_in的index大于BlockNode的EndOffset,那么将BlockNode的EndOffset设置成Info_in的index,然后调用zkserver的zk的RegisterNode函数也就是对他进行更新，然后调用zkserver的zk的GetDuplicateNode函数，传入参数：str+"/"+Info_in的cli_name,得到DuplicateNode,将DuplicateNode的EndOffset设置成Info_in的index，然后调用zkserver的zk的RegisterNode进行更新