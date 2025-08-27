CloseFetchPartiiton(...)参数:

topic,part

内部调用CloseFetchPartiitonHandle函数,传入参数Info:topic,part
首先构造str:=Info的topic+part+filename(这是从哪来的),然后再 *server的parts_fetch中找str是否存在,要是不存在,那么就直接返回,要是存在,那么就从 *server的parts_fetch中将这个str删除