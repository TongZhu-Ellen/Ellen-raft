# Goroutine起收的那些事

按单元逐个说,每个单元固定回答四件事:几个、谁启动、怎么死、Kill 时管不管。

## ① ticker

- **数量**:1/Raft
- **启动**:`Make` 里 `go rf.ticker()`开启。
- **怎么死**: 每轮睡 50~350ms,醒来发现 `dead==1` 就退出。
- **我的个人看法**:低入侵。快速回收（最多350ms就回收了）。没毛病。

## ② applier

- **数量**:1/Raft
- **启动**:Make 里  `go rf.applier() `开启。
- **怎么死**: 这个和ticker不一样的地方在于，它不是自轮询的；它是有活了才会被rf.applyCond叫醒的。然后我让rf.Kill()在锁里面也提供叫醒。醒来之后检查一次是否死亡。
- **内存泄漏风险点**：一旦`for _, apply := range applies {
			rf.applyCh <- apply // 这里阻塞，例如上层停止消费 applyCh
		}` 那么就跑不到检查Raft是否还活着的那个点了。



  

## ③ replicator

- **数量**:每任 leader 创建 N-1 个,每个 follower 对应 1 个。
- **启动**:`becomeLeader` 时创建,每个 replicator 对应一个 follower。
- **怎么死**:leader 退任或 Raft死亡时结束,被 `ctx.Done()` 赐死。实际运行时,我们按照 start 的指示以及 ticker 的指示进行发送。其中，等待发送的过程随时可以被打断,无限重试的looping间隙也可以被打断,但是一旦决定发 RPC那么它将不会受到类似打断。
- 哦对了，这里引入ctx只是为了方便回收goroutine. RPC的发送实际上是三段式的。不存在“打断PRC的发送和处理的过程以另类实现raft安全性”的创举..... 莫要误会。
- **我的个人看法**:replicator 基本上在所有可能无限滞留地方都留了退出路径:等待发送可以打断,retry 的间隙可以打断；唯一已经开始、无法被 ctx 打断的 RPC,本身又有 timeout。这里的安全性也比较高。






## ④ collectOpinion 与投票 goroutine

- **数量**:每次选举 1 个协调者(`collectOpinion`) + 它负责派出的 N-1 个投票 goroutine。协调者派完活就结束, 投票 goroutine 等待直到RPC回包。

- **我的个人看法**: 哈哈哈这种goroutine的本质是一个完整的RPC周期。依托于RPC的timeout机制所以会在有限时间内返回。不过这也造成了一个开始我认为很奇观的景象：raft已经死亡但是PRC的回复逻辑却还在跑。Raft论文难懂的原因之一。另外：Kill 之后的迟到回包是模拟环境的产物,所以我故意没去处理... 

