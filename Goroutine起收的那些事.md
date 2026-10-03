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
- **我的个人看法**:replicator 基本上在所有可能无限滞留地方都留了退出路径:等待发送可以打断,retry 的间隙可以打断；唯一已经开始、无法被 ctx 打断的 RPC,本身又有 timeout。也就是说, 像防止自然灾害一般防止goroutine泄露。







## ④ collectOpinion 与投票 goroutine

- **数量**:每次选举 1 个协调者(`collectOpinion`),它再派出 N-1 个投票 goroutine。协调者派完活就退出,只有投票 goroutine 继续跑。
- **启动**:`ticker` 判定超时、`becomeCandidate` 之后 `go rf.collectOpinion(args)`。
- **怎么死**:每个投票 goroutine 发一次 `RequestVote`,回包后做一次带校验的计票(`reply.Term`、`state == Candidate`、`currentTerm == args.Term`),然后退出。
- **Kill 管不管**:不管,也不需要管。它们的生命周期就是一次 RPC,`Call` 返回就结束。

## ⑤ singleAppend 里的 RPC goroutine

- **数量**:每次 `singleAppend` 调用 1 个。
- **启动**:`singleAppend` 构造好 args 之后,放进 goroutine 里发 RPC。
- **怎么死**:主流程用 `select` 等结果或等 200ms 超时。超时后主流程放弃,但那个 goroutine 还在等 `Call` 返回。`okCh` 容量是 1,所以它返回后能无阻塞地写入然后退出。
- **Kill 管不管**:不管。没有 ctx 管它，它靠 Call 自己返回。

## 一眼汇总

| 单元 | 数量 | Kill 时 |
|---|---|---|
| ticker | 1 / 节点 | 被动退出,≤350ms |
| applier | 1 / 节点 | 主动唤醒后退出 |
| replicator | N-1 / 每任 leader | 若是 leader 则 cancel |
| 投票 goroutine | N-1 / 每次选举 | 自行随 RPC 结束 |
| RPC goroutine | 1 / 每次发送 | 自行随 RPC 结束 |

前两个是常驻的,生命周期跟节点走。replicator 跟 leader 任期走。后两种跟一次 RPC 走。四类生命周期各用了不同的回收机制:标志位加轮询、条件变量唤醒、context 取消、靠 RPC 自然返回。
