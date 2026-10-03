# Goroutine起收总结

按单元逐个说,每个单元固定回答四件事:几个、谁启动、怎么死、Kill 时管不管。

## ① ticker

- **数量**:每个 Raft 节点 1 个。
- **启动**:`Make` 里 `go rf.ticker()`。
- **怎么死**:循环条件是 `for rf.killed() == false`。每轮睡 50~350ms,醒来发现 `dead==1` 就退出。
- **Kill 管不管**:不主动管。`Kill` 只置标志,不唤醒它,它靠自己下次醒来发现。所以回收延迟最多约 350ms。

## ② applier

- **数量**:每个节点 1 个。
- **启动**:`Make` 里 `go rf.applier()`。
- **怎么死**:它睡在 `applyCond.Wait()` 上。每次醒来先查 `killed()`,是就解锁 return,再查有没有活。
- **Kill 管不管**:主动管。`Kill` 先置 `dead`,再持锁 `Signal` 唤醒它。因为"置标志"和"Signal"之间 `Kill` 要拿锁,而 applier 检查标志和进入 `Wait` 是在同一把锁里完成的,所以不会出现"刚检查完还没睡就错过通知"的情况。
- **边界**:applier 往 `applyCh` 发送是在锁外进行的。如果上层不再读 `applyCh`,它会卡在发送上,Kill 唤醒不了它。这取决于上层是否一直消费。

## ③ replicator

- **数量**:每任 leader 创建 N-1 个,每个 follower 对应 1 个。
- **启动**:`becomeLeader` 里创建一个 `context.WithCancel`,cancel 存进 `rf.leaderCancel`,再逐个 `go rf.replicator(i, ch, ctx)`。同一任的所有 replicator 共用这一个 ctx。
- **怎么死**:`select` 收到 `ctx.Done()` 就 return。若它正在 `appendLoop` 里,`singleAppend` 开头会检查 `state != Leader`,返回 false 跳出循环,然后回到 `select` 看到 ctx 已取消,退出。
- **Kill 管不管**:管,但有条件。`Kill` 里如果 `state == Leader` 就调 `leaderCancel()`。降级也是同一个出口:`toFollower` 判断之前是 leader 就 cancel。被高 term 打下台、收到同 term 的 AppendEntries、`Kill`,三条路径都走这一个 cancel。
- **边界**:如果 `Kill` 发生时节点是 candidate,之后某个投票回包到达并触发 `becomeLeader`,这一任 replicator 会在 Kill 之后才创建,没人 cancel。`singleAppend` 只检查 `state`,不检查 `killed()`。这是个窗口很小的泄漏,我建议你在 `becomeLeader` 或 `singleAppend` 里加一句 `killed()` 检查来补上。

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
