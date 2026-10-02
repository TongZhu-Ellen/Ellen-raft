package raft 

import "time"
import "context"

/*
持锁的一个子行为。
在论文里面对应的是 If RPC request or response contains term T > currentTerm:
set currentTerm = T, convert to follower 

这个函数本身没有锁！只能锁内调用！
*/


func (rf *Raft) toFollower() {

	
	if rf.state == Leader {
        rf.leaderCancel()
    }
	rf.state = Follower
	

}

func (rf *Raft) becomeCandidate() {

	rf.currentTerm++
	rf.lastTouchedAt = time.Now()	
	rf.state = Candidate
	rf.votedFor = rf.me
	
	rf.persist()
}


func (rf *Raft) becomeLeader() {
	rf.state = Leader

	lastLogIndex := rf.logLength() - 1
	ctx, cancel := context.WithCancel(context.Background())
	rf.leaderCancel = cancel

	rf.repliCh = make([]chan struct{}, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))  
	rf.matchIndex = make([]int, len(rf.peers)) 

	for i := range rf.peers {
		if i == rf.me {
			rf.nextIndex[i] = -1
			rf.matchIndex[i] = -1
			continue
		} 
		
		rf.repliCh[i] = make(chan struct{}, 1)
		rf.nextIndex[i] = lastLogIndex + 1 // "initialized to leader's lastLogIndex + 1"
		rf.matchIndex[i] = 0 // "initialized to 0"
		go rf.replicator(i, rf.repliCh[i], ctx)
	}

	go rf.leaderTicker(ctx, rf.repliCh)



	
}

func (rf *Raft) leaderTicker(ctx context.Context, repliCh []chan struct{}) {
    ticker := time.NewTicker(HEATBEAT_INTERVAL)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return

        case <-ticker.C:
            rf.allReplicatorGo(repliCh)
        }
    }
}


func (rf *Raft) newGen(term int) { 

	rf.currentTerm = term
	rf.toFollower()
	rf.votedFor = -1

	rf.persist()
}





func (rf *Raft) tryVotingFor(candidate int, lastLogIndex int, lastLogTerm int) bool {
	
	myLastIndex := rf.logLength() - 1
	myLastTerm := rf.get(myLastIndex).Term
	
	
	upToDate := lastLogTerm > myLastTerm || 
            (lastLogTerm == myLastTerm && lastLogIndex >= myLastIndex)

	if (rf.votedFor == -1 || rf.votedFor == candidate) && upToDate {
		
		
		rf.votedFor = candidate
		rf.persist()
		rf.lastTouchedAt = time.Now()
		
		return true
	}

	return false
}

func (rf *Raft) touch() {

	rf.lastTouchedAt = time.Now()
}

func min(a int, b int) int {
	if a <= b {
		return a
	} 
	return b
}




