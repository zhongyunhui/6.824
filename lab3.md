## 6.824 lab3

#### 客户端如何和Raft交互
Raft中的客户端发送所有请求给leader。当客户端启动时，随机挑选一个服务器进行通信。当客户端第一次挑选的服务器不是leader，则服务器会拒绝客户端的请求并且提供它最近接收到的leader的信息。如果leader崩溃，则客户端请求超时；之后客户端会再次重试随机挑选服务器的过程。

Raft目标是实现线性化语义：每一个操作立即执行，只执行一次，再调用和收到回复之间。但Raft是可以执行同一条命令多次的，当leader提交了该日志后，再响应前崩溃，客户端和新leader会重试指令，导致这条命令被再次执行。解决方法：客户端对于每一条指令都赋予一个唯一的序列号。然后状态机跟中每条指令最新的序列号和相应的响应。如果接收到一条指令，它的序列号已经被执行，则立即返回结果，而不重新执行。

只读的操作可以直接处理而不需要记录日志。但是可能会冒着返回脏数据的风险，因为leader响应客户端请求时，可能已经被新的leader作废了，但它并不知道。所以要加以设置：
1. leader必须有关于被提交日志的最新信息。为了知道这些信息，Raft需要在任期里提交一条日志条目，Raft中通过leader在任期开始时候提交一个空白的没有任何操作的日志条目到日志中来实现。
2. leader在处理只读的请求之前必须检查自己是否已经被废除了。Raft中通过让leader在响应只读请求之前，先和集群中的大多数节点发送一次心跳来处理该问题。
#### 说明

键值服务是一个赋值状态机，由几个使用Raft进行赋值的键值服务器组成。只要大多数服务器都是活的，能够通信，就能继续处理客户端请求。在lab3之后，要实现Raft 交互图中显示的所有部分。（Clerk、service和Raft）。

service支持三个操作：put，append和get。维护一个简单的k/v数据库。key和value都是字符串。

put替换数据库中特定键的值，Append向key的值追加arg，get获取键的当前值。对不存在的键，Get返回一个空字符串。对不存在的键进行Append应该和Put类似。每个客户端通过一个具有Put、Append、Get方法的Clerk与服务进行对话。一个Clerk管理RPC与服务器的交互。



service提高对Clerk Get、put、append方法的应用程序调用的强一致性。Get、Put、和Append方法的行为应该像是系统只有其状态的一个副本。每个调用都应该观察到前面调用序列





每个kv服务器都有一个关联的Raft对等点。Clerks发送Put、Append和GetRPC给和leader的Raft关联的kvserver。



kvserver的code将put、append和get操作发送给Raft，因此Raft日志保存了一个Put、Append和Get的操作序列。所有的kvserver都按顺序执行来自Raft日志的操作，并将这些操作应用到她们的键值数据库，目的是让服务器维护键值数据库的相同副本。

clerk有时候不知道哪个kvserver是raftleader。当clerk将RPC发送到错误的kvserver或到达不了kvserver，clerk应该通过发送到另一个kvserver进行重新尝试。如果kv service将操作提交到其Raft日志，则leader通过响应其RPC将结果报告给Clerk；如果失败，server将报告一个错误，clerk将用另一个服务器重试。

kvservers不应该直接通信；只能通过Raft进行交互

### 任务

* 第一个任务是实现一个解决方案，该解决方案在没有被删除的消息和没有失败的服务器的情况下工作。

* 需要增加RPC发送代码给Clerk的put、append和get方法在client.go中。实现PutAppend和Get RPC handlers在server.go中。
* 这些handlers应该使用Start()输入一个0p在Raft的log中。应该在server.go中填写0p的结构定义来描述Put、Append和Get操作。当Raft提交0p命令时，每个服务器都应该执行她们，当它们出现在applyCh时。
* RPC handler应该注意到Raft何时提交了它的0p，然后回复RPC。



0 put 1 4132094400