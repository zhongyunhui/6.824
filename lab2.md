## 6.824 Lab2：Raft

目标：构建一个容错键值存储系统。这个实验中，实现Raft，一个复制的状态机协议。在下一个实验中，您将在Raft之上构建一个键/值服务。然后，您将通过多个复制状态机对您的服务进行“分片”，以获得更高的性能。

一个副本服务通过在多个副本服务器上存储其状态（数据）的完整副本来实现容错。复制允许服务继续进行，即使它的一些服务器出现故障。问题是，失败可能导致副本具有不一致性。

Raft将客户端请求组织为一个序列，称为日志，并确保所有的副本服务器看到相同的日志。每个副本按日志顺序执行客户端请求，将它们应用于服务状态的本地副本。如果服务器故障但恢复，Raft会将日志更新为最新状态。只要大部分服务器存活并彼此通信，Raft就继续运行。

该实验中，实现Raft实现为带有关联方法的作为GO的对象类型，用于大型服务中的模块。一组Raft实例使用RPC相互通信来维护复制的日志。Raft接口要支持不确定的sequence of number commands，也叫做log条目。条目用索引号进行编号。具有给定索引的log entry最终被提交。此时，Raft应该将log entry发送给更大的服务去执行。

遵循Raft论文中的设计，特别关注图2.将实现论文中的大部分内容。包括保存持久状态并在节点失败后读取它，然后重新启动。

guide有帮助，而且有关于locking和并发结构的建议。part 2D部分已经改变。

Raft交互图帮助解析Rafy代码如何与其上层交互

#### 编写代码

实现raft.go

##### 以下接口

1. 一个服务调用Make(peers,me...) 创建Raft对等点。peers参数是Raft peers的一组network identifiers。me是在peers array中该peer的编号。
2. Start(command)告诉Raft去启动进程，将command输入到replicated log中。Start方法应该立即运行，需要等待输入到log中的过程完成。
3. 该服务希望你实现给每一个新提交的log entry一个ApplyMsg到applyCh通道参数到Make中。

你的Raft peers应该使用labrpc GO包来交换RPCs。Tester 通知labrpc去延迟RPC，重新排序他们，并丢弃他们来实现各种网络失败。你的Raft实例必须只和RPC交互；不允许使用共享的Go变量或文件进行通信。

##### 实验内容

Part 2A：leader选举

实现Raft leader选举和心跳检测（使用没有log entries的AppendEntries RPC实例）。Part 2A的目标是选出一个单一的领导，如果旧领导失败或to/from旧领导的数据包丢失，一个新的领导去接管。go test -run 2A -race去测试。

#### 暗示

1. 通过tester去运行Raft实现，go test -run 2A -race
2. 参照paper的图2，此时关心的是接收和发送投票请求的RPC实例，给服务器写与选举相关的规则，和与领导选择相关的状态
3. 将图2的状态添加到Raft.go的Raft结构中。还需要定义一个结构体来保存关于每个log entry的信息。
4. 填写RequestVoteArgs和RequestVoteReply结构体，修改Make去创建一个后台go线程，当它还没有收到另一个peer的消息时，通过发送RequestVote rpc来定期启动leader选举。通过这种方式，peer会知道谁是leader当已经有一个leader或称为leader本身。实现RequestVote() RPC处理程序以便服务器将为彼此投票。
5. 要实现heartbeats，定义一个AppendEntriesRPC结构体（尽管可能还不需要所有的参数），并让leader定期发送它们。编写一个AppendEntries RPC处理方法，该方法重置选举超时时间使得其他的服务器在一个已经被选择出来的时候不会站出来做leader。
6. 确保不同peers的选举超时时间不总是同时fire，否则所有同伴只会为自己投票，没人成为领导者。
7. tester要求leader每秒发送心跳RPC不超过10次。
8. tester要求Raft在旧leader失败后的5s内选出新leader。为了防止选票分裂，leader选举可能需要多轮投票（(如果数据包丢失或候选人不幸选择了相同的随机退步时间，可能会发生这种情况)）。必须选择足够短的leader超时（以及心跳intervals），以便选举在5s内完成，即使它需要多轮。
9. 由于发送心跳次数不超过每秒10次。因此要使用大于论文中150ms到300ms的选举超时时间，但不能太大，不然无法在5s内选出leader。
10. GO的rand有用
11. 需要编写定期或延后执行操作的代码。最简单的方法是创建一个调用time.Sleep的go线程循环。（查看Make()创建的tricker的goroutine）
12. Guidance page有一些关于如何开发和调试代码的提示
13. 如果您的代码无法通过测试，请再次阅读本文的图2;领导人选举的全部逻辑分布在图的多个部分
14. 不要忘记实现GetState()方法
15. 当永久关闭一个实例时，tester调用Raft的rf.Kill（)。你可以检查Kill（）是否被rf.killed()调用。你可能希望在所有循坏中都这样做，以避免已经死掉的Raft实例打印令人困惑的消息。
16. Go的RPC只发送名称以大写字母开头的结构字段。子结构还必须有大写的字段名(例如，数组中日志记录的字段)。labgob包将发出警告，不要无视。

确保你通过了2A测试

每个Passed行包括5个数字：测试所花费的秒数、Raft peers的数量、RPC发送的数量、RPC 消息中总字节数以及Raft报告所提交的log entries的数量。





#### lab2B

当Raft启动时候:

```
server:
	state: Follower
	currentTerm: 0
	votedFOr: -1
	log[]: [占位entry]  // 使得新添加的log的index从1开始
	commitIndex: 0
	lastApplied: 0
```

当electimeout之后，变成candidate向其他节点发送requestVote RPC，term > rf.currentTerm && candidate的log至少和receiver的log一样最新授予投票。

当变成leader之后，初始化nextIndex[]和matchIndex[]，并向其他节点立即发送心跳。

异步地，当lastLogIndex>=follower的nextIndex时，对该follower发送appendEntries请求，index是从nextIndex开始发送。

client发送command给leader，当leaderapplied state machine时，应用到状态机并回复给client。

lab2C：

如果基于Raft的服务器重新启动，它应该在中断的地方恢复服务，所以要存在持久化状态。

真正的实现是在每次Raft发生更改时将持久状态写入磁盘，并在重新启动后从磁盘读取状态。

它从一个Persister对象中保存持久化状态。无论谁调用Raft.Make()，都将提供一个最初保存Raft最近持久化状态的persistent。并且在每次状态改变时，使用它保存它的持久化状态。使用Persister的ReadRaftState和SaveRaftState方法。

完成persist()和readPersist()来保存和恢复持久化状态。需要encode或者serialize状态为字节数组传给Persister。使用labgob encoder。labgob类似于Go的gob编码器，如果试图使用小写字段名编码结构，则会打印错误信息。

在改变持久化状态时，插入对persist的调用。

lab2D:

修改Raft：服务将不时地持久化存储其当前状态的快照，而Raft将丢弃快照之前的日志条目。当一个service远远落后与leader并且需要catch up，service首先安装一个snapshot，然后在创建快照之后从创建快照的点开始重演日志条目。第七节概述了该方案。

从图里理解复制服务和Raft的通信。

为了支持snapshot，需要服务和Raft库之间的接口。

服务调用Snapshot()将其状态的快照传递给Raft。快照包含索引之前的所有信息。快照包含索引之前的所有信息。这意味着相应的Raft peer不再需要日志throung and including index。您的Raft实现应该尽可能地修剪它的日志。要修改Raft代码，以便于只存储日志的尾部。

Raft leader有时必须告诉滞后的Raft peer通过安装一个snapshot来更新她们的状态。需要实现InstallSnapshot RPC senders和handler来安装快照。与AppendEntries相反，后者发送日志条目，由服务逐个应用这些日志条目。

InstallSnapshot rpc是在Raft peers间发送，而服务使用Snapshot/CondInstallSnapshot提供的骨架函数来与Raft通信。

当follower接收并处理InstallSnapshot RPC时，必须使用Raft将包含的快照交给服务。InstallShapshot handler 可以使用applyCh将快照发给服务，通过将snapshot放在ApplyMsg中。service 从applyCh 中读取数据，并使用snapshot调用CondInstallShapshot告诉Raft，该服务正在切换到传入的快照状态，并且Raft应同时更新其日志。

CondInstallSnapshot应该拒绝安装一个snapshot如果它是一个老的snapshot。（如果Raft已经有了处理后的entries，在snapshot的lastIncludedTerm/lastIncludedIndex）。因为Raft可能在处理InstallSnapshot RPC之后，在服务调用CondInstallSnapshot之前，处理其他RPC并在applyCh上发送消息。

Raft不能返回较旧的快照，所以必须拒绝较旧的快照。当实现拒绝快照时，CondInstallSnapshot应该返回false，这样服务就知道它不应该切换到快照。

如果snapshot是最近的，那么Raft应该修建它的日志，持久化新状态，返回true，并且service应该在处理applyCh上的下一条消息之前切换到snapshot。

CondInstallSnapshot设计允许您的实现检查快照是否必须安装在一个地方，并自动地将服务和Raft切换到快照。

#### 任务

修改Raft代码来支持snapshot：实现Snapshot，CondInstallSnapshot和InstallSnapshot RPC以及对Raft的修改（例如，继续使用修剪过的日志。）

Hint：

1. 在单个InstallSnapshot RPC中发送整个快照。不要实现offset机制来分割快照。
2. Raft必须以允许Go垃圾收集器释放和重用内存的方式丢弃旧的log entries；这要求对丢弃的日志项没有可到达的引用
3. Raft的日志不能再使用log entries的位置或log的长度来确定log entry的索引；您需要使用独立于log position的索引模式。
4. 即便当日志呗修剪过，您的实现仍然需要再AppendEntries RPC中的新条目之前正确发送entries的term和index。这可能需要保存和引用最新快照的lastIncludedTerm/LastIncludedIndex（考虑是否应该持久化）。
5. Raft必须使用SaveStateAndSnapshot将每个快照存储再持久化对象中。