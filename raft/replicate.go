package raft 

import "time"
import "context"



type AppendEntriesArgs struct {

	Term int // leader's term
	LeaderId int

	// 2B:
	PrevLogIndex int // 上次的最后一条，
	PrevLogTerm int 
	Entries []Entry
	LeaderCommit int

}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	
	Term int // my term / follower's term
	
	// 2B:
	Success bool
	

}










// helper func; can only be called by leader!
func (rf *Raft) replicator(i int, ch chan struct{}, ctx context.Context) {
	


	ticker := time.NewTicker(HEATBEAT_INTERVAL)
	defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return

		case <-ch:
			rf.appendLoop(i, ctx)

        case <-ticker.C:
            rf.appendLoop(i, ctx)
        }
    }
}

func (rf *Raft) appendLoop(i int, ctx context.Context) {
	for rf.singleAppend(i) {
		
		select {
		case <-ctx.Done():
			return
		default:
		}

	
	}
}




/*
singleAppend 给 follower i 做一次 Log Replication。

返回值 retry 表示：
- true：这次没有完成 replication，需要 appendLoop 立即再次调用 singleAppend。
  典型情况：RPC 失败、Log 不一致需要调整 nextIndex 后重试。
- false：这次不需要立即重试，等下一次 heartbeat/replication 周期。
  
*/
func (rf *Raft) singleAppend(i int) (retry bool) {
	
	rf.mu.Lock() // ----------- 锁 --------------
	if rf.state != Leader {
		rf.mu.Unlock()
		return false
	}
	
	prevLogIndex := rf.nextIndex[i] - 1


	
    args := &AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: rf.get(prevLogIndex).Term,
		Entries: rf.entriesFrom(prevLogIndex+1), 
		LeaderCommit: rf.commitIndex,
	}
    reply := &AppendEntriesReply{}
    rf.mu.Unlock() // ----------- 锁 --------------

    // 原来是 ok := rf.sendAppendEntries(i, args, reply)
	okCh := make(chan bool, 1)
	go func() {
		okCh <- rf.sendAppendEntries(i, args, reply)
	}()

	var ok bool
	select {
	case ok = <-okCh:
	case <-time.After(APPENDPRC_TIMEOUT):
		ok = false
	}

    // ----------- Server 处理中！ --------------

	if !ok { // 这是没发出去...  
		time.Sleep(10 * time.Millisecond)
		return true
	}

	


	rf.mu.Lock() // ----------- 锁 --------------
    defer rf.mu.Unlock()

    // 更改自身term的逻辑永远先行！
    if reply.Term > rf.currentTerm { 
		rf.newGen(reply.Term)
		return false
	}

	// 朝代已然改变！
    if rf.currentTerm != args.Term || rf.state != Leader {
		return false
	}


	// "If AppendEntries fails because of log inconsistency: decrement nextIndex and retry"
    // "If successful: update nextIndex and matchIndex for follower"
    if !reply.Success {
    rf.nextIndex[i]--
    return true

		
}



	
	// 不变量:每个 follower 只有一个 replicator goroutine,且 appendLoop 内串行发送,
	// 所以同一个 follower 的回包不会乱序交错,这里直接赋值不会让 matchIndex 回退。
	// 如果以后改成并发发送(比如流水线),必须改成 max(旧值, 新值)。
	
	rf.matchIndex[i] = prevLogIndex + len(args.Entries)
	rf.nextIndex[i]  = rf.matchIndex[i] + 1
	rf.updateCommitIndex()
	return false
}




























 

func (rf *Raft) sendAppendEntries(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
	return ok
}


























// example AppendEntries RPC handler.
// 这是follower方的处理函数！
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).


	rf.mu.Lock()
	defer rf.mu.Unlock()

	//  --------------  全局条 --------------
	if args.Term > rf.currentTerm { 
		// "If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower"
		rf.newGen(args.Term)
	} else if rf.state != Follower && args.Term == rf.currentTerm {
		// "If AppendEntries RPC received from new leader: convert to follower"
		rf.toFollower()
	}

	

	//  ---------------- 专属条们 -----------------

	// 1. "Reply false if term < currentTerm"
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return 
	}

	rf.touch()

	
	lastNewIndex := args.PrevLogIndex + len(args.Entries)



	


	// 2. "Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm"
	if rf.logLength() <= args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.get(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}



	// 3. "If an existing entry conflicts with a new one (same index but different terms), 
	// delete the existing entry and all that follow it"
	// 4. "Append any new entries not already in the log"

	rf.reconcileEntries(args.PrevLogIndex + 1, 0, args.Entries)
	

	
	

	// 5. "If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)"
	rf.tryUpdateCommit(args.LeaderCommit, lastNewIndex)


	

	
   

	reply.Term = rf.currentTerm
	reply.Success = true

	

} 
