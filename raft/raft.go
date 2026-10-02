package raft

import (
	
	"sync"
	"sync/atomic"
	"time"
	"6.5840/labrpc"
)





func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower

	rf.lastTouchedAt = time.Now() // 一上来就触发选举很显然是不对的。
	rf.log = []Entry{Entry{Term: -1}} // first index is 1 


	
	


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}








func (rf *Raft) GetState() (int, bool) {

	if rf.killed() {
		return -1, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}









// Entry制造入口！
func (rf *Raft) Start(command interface{}) (int, int, bool) {


	rf.mu.Lock()
	defer rf.mu.Unlock()


	if rf.state != Leader {
		return -1, -1, false
	}

	entry := Entry{
		Term: rf.currentTerm,
		Command: command,
	}
	
	
	rf.append(entry)

	rf.allReplicatorGo()
	

	index := rf.logLength() - 1 // index to be inserted to! 
	term := rf.currentTerm
	isLeader := true

	

	return index, term, isLeader
}















func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.applyCond.Signal()
	
	if rf.state == Leader {
		rf.leaderCancel()
	}
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}























func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 2D 不实现，空着
}




