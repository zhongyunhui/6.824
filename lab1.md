## 任务

实现一个分布式mapreduce，包括coordinator和worker。

worker向coordinator请求任务来从一个或多个文件中读取任务的输入，并将任务输出写入一个或多个文件。worker没在10秒内完成任务，coordinator将同样的任务给别的worker

把实现放在mrcoordinator.go，mr/worker.go和mr/rpc.go

每个文件对应于一个切片，是一个map任务的输入

test-mr.sh 将检查wc和indexer Mapreduce应用程序产生正确的输出。测试还检查您的实现是否并行运行Map和Reduce任务，以及您的实现是否从运行任务时崩溃的工作者中恢复。

每个reduce task输出的文件名字为mr-out-X

忽略Done错误，将coordinator注册为RPC服务器会检查它的方法和RPCs相吻合，我们直到Done不是通过RPC调用的

#### 一些规则

* map阶段应该将中间键划分为桶给n个Reduce的recude任务，nReduce是main/mrcoordinator.go传递给MakeCoordinator的参数
* 第x个reduce任务产生mr-out-X文件
* mr-out-X文件应该在每个Reduce函数输出中包含一行。这一行应该用Go的%v,%v格式化，命名为key和value。看下main/mrsequential.go的line是正确的格式
* worker将中间Map输出放到当前目录中的文件中，worker以后读取它们作为Reduce任务的输入
* main/mrcoordinator.go 希望mr/coordinator.go去实现一个Done方法，返回true，当MapReduce任务完全完成，这样mrcoordinator.go会退出
* 当job完全执行后，辅助进程应该退出，使用call函数的返回值；如果worker未能与coordinator联系，则worker终止。也可以发送设置一个请退出的伪任务给worker

#### 暗示

* 一个启动的办法是修改mr/worker.go里的Worker来发送一个RPC给coordinator请求任务。然后修改coordinator去使用尚未启动的map任务。然后修改worker去读取文件并调用在mrsequential.go中的Map函数

* 应用程序的Map和Reduce函数是在运行时用Go插件包从文件名以so结尾的文件中加载的

* 如果你改变mr/目录下的任何东西，你可能需要重新构建你使用的任何MapReduce插件，比如go build -race -buildmode=plugin ../mrapps/wc.go

* 这个实验室依赖于工人共享一个文件系统。当所有工作人员运行在同一台机器上时，这很简单，但如果工作人员运行在不同的机器上，则需要像GFS这样的全局文件系统。

* 中间文件的合理命名约定是mr-X-Y，其中X是Map任务号，Y是Reduce任务号

* worker的map将需要一种方法来在文件中存储中间键值对，使用encoding/json包，将键值对写入json文件

  ```go
    enc := json.NewEncoder(file)
    for _, kv := ... {
      err := enc.Encode(&kv)
        
    
     dec := json.NewDecoder(file)
    for {
      var kv KeyValue
      if err := dec.Decode(&kv); err != nil {
        break
      }
      kva = append(kva, kv)
    }
  ```

* worker的map部分可以使用worker.go中的ihash函数为给定的key选择reduce任务

* 您可以从mrsequential窃取一些代码。go用于读取Map输入文件，用于排序Map和Reduce之间的中间键/值对，以及用于在文件中存储Reduce输出。

* 作为RPC服务器的协调器将是并发的;不要忘记锁定共享数据。

* 使用Go的比赛检测器，带有Go build -race和Go run -race。默认情况下，Test-mr.sh使用竞争检测器运行测试。

* worker有时需要等待，例如reduce直到最后一个map完成后才开始。一种是worker定期向coordinator请求工作，并在每个请求之间随time.sleep睡觉。另一种是coordinator中的相关RPC运行处理程序有一个等待的循环，time.Sleep或sync.Cond。GO在其自己的线程中为每个RPC运行处理程序。一个处理程序正在等待的事实不会阻止coordinator处理其他rpc。

* 对于区分worker是否是崩溃的，让coordinator等待10s，假定该worker死亡，然后放弃并将任务重新发送给另一个worker

* 如果您选择实现备份任务(第3.6节)，请注意，我们测试您的代码不会安排额外的任务时，当worker执行任务而不崩溃。备份任务只应该在一段相对较长的时间后调度，10s后

* 要测试崩溃恢复，可以使用mrapps/crash。应用程序的插件。它随机存在于Map和Reduce函数中

* 为了确保在崩溃的情况下没有人观察到部分写入的文件，MapReduce论文提到了使用临时文件并在完全写入后自动重命名它的技巧。你可以用ioutil.TempFile创建临时文件和os.Rename重命名为原子重命名。

* test-mr - sh运行子目录mr-tmp中的所有进程，所以如果出现错误，并且您想查看中间文件或输出文件，请查看那里。您可以修改test-mr.sh，使其在失败的测试后退出，这样脚本就不会继续测试

* test-mr-many.sh提供了一个用于超时运行test-mr- sh的基本脚本(这就是我们测试代码的方式)。它将运行测试的次数作为参数。您不应该并行地运行多个test-mr - sh实例，因为协调器将重用相同的套接字，从而导致冲突。

#### 流程

1. Worker向Coordinator请求任务，当任务的state为0或者state=1且当前时间>开始时间加10时，则将该任务给Worker，一开始给map任务，当map任务完成后，判断mapfinish是否为true，若为true，则给reduce任务，如果map和reduce任务都在跑的话，则新建一个任务给该任务的类型为wait，让worker等待
2. 当worker接收到map任务后执行map任务，并发送给Coordinator任务完成和该任务实例
3. 当Worker接收到reduce任务后执行reduce任务，并发送给Coordinator任务完成和该任务实例
4. Coordinator接收到该任务，将该任务的state修改为2，并判断是否所有的map任务和reduce任务完成，若所有任务完成，则把mapfinish和reducefinish标记为true