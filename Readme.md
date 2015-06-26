Kpark
--
version 0.1^n

这是一个写着玩的基于Tornado和CloudPickle分布式弹性数据集计算工具。

translate: 
> This is a "Resilient Distributed Datasets" compute framework based on Tornado and cloudpickle write by fun.

只是一个样例。很多东西可以继续补充写。

> it is just a demo with no full completed.

实现了想插就插的邪恶思想，但是还没有实现容错。就是可以随时无限扩张计算网络。效率没测 - -。反正单机24个Client进程跑5万每秒的流式，计算一大堆正则和归并1秒内搞定没什么问题。

> my english is damn pool...

example
--

```python
import rdd
import Server
Server.initServer()
Server.initIO()

def run():
    print rdd.RDD(range(1000),ctx=Server.Context).filter(lambda x:x%2==0).map(str).collectAsMap()

if __name__=='__main__':
	run()
	while True:
		pass

```

如果没有计算客户端连接到Server，那么就切换成本机运算。

否则有就切换到远端计算，而Server本身只处理一些聚合类的工作。

程序里弱化了RDD中关于Transformer的概念...

据说代码搓的人长得就帅，我就试试...

> if there are no computeClient connected. this compute will change to local.
> else, it will send the data to remote computeClient and the Server just do something aggregation。
> in Kpark, it has less Transformer type in RDD, i just wanna do it simple.
> and seems the badcode makes coder handsome. hmmm yeah. :D