package raft


// 这些全部线程不安全的！
// I do not lock in helpers! as in 2A!










func (rf *Raft) allReplicatorGo() {
    for i := range rf.peers {
        if i == rf.me {
            continue
        }

        select {
        case rf.repliCh[i] <- struct{}{}:
        default:
        }
    }
}







/*

	If there exists an N such that N > commitIndex, a majority
	of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N



					  idx=1  idx=2  idx=3
					 <<<---------------
					┌──────┬──────┬──────┐
			 A      │      │  ✓   │  ✓   │
			 B      │      │  ✓   │      │
			 C*     │  ✓   │  ✓   │  ✓   │
			 D      │      │      │      │
					├──────┼──────┼──────┤
			 count  │      │  3   │  2   │
			    	└──────┴──────┴──────┘
							✓bingo!




*/
// leader专属函数
func (rf *Raft) updateCommitIndex() {
    // 从最新日志往前找，寻找可以提交的最大 N
    for N := rf.logLength() - 1; N > rf.commitIndex; N-- {
        count := 0
        for i := 0; i < len(rf.peers); i++ {
            if i == rf.me {
                count++ // 自己也算一票
                continue
            }
            if rf.matchIndex[i] >= N { // 该节点已复制到 N
                count++
            }
        }
        // 多数派已复制 且 该条目属于当前任期（Raft 安全性要求）
        if count > len(rf.peers)/2 && rf.get(N).Term == rf.currentTerm {
            rf.commitIndex = N // 推进 commitIndex
            rf.applyCond.Signal()
            break              // 找到最大的 N 即可，立即停止
        }
    }
}









// reconcileEntries 从 myIdx（本地日志全局index）和 yourIdx（entries切片下标）开始，
// 将incoming entries与本地日志逐条比对：遇到term冲突则截断本地日志，
// 最后将剩余未处理的entries追加到本地日志。
func (rf *Raft) reconcileEntries(myIdx int, yourIdx int, entries []Entry) {
    for myIdx < rf.logLength() && yourIdx < len(entries) {
        if rf.get(myIdx).Term != entries[yourIdx].Term {
            rf.log = rf.log[:myIdx]
            break
        }
        myIdx++
        yourIdx++
    }
    rf.batchAppend(entries[yourIdx:])
}

// 调用时需持有 rf.mu
// lastNewIndex: 本次 RPC 验证过的最后一条 entry 的全局 index
func (rf *Raft) tryUpdateCommit(leaderCommit, lastNewIndex int) {
    if leaderCommit > rf.commitIndex { // 否则不需要改！
        newCommit := min(leaderCommit, lastNewIndex)
        if newCommit > rf.commitIndex { 
            rf.commitIndex = newCommit
            rf.applyCond.Signal()
        }
    }
}




