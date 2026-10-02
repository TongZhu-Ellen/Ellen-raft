package raft 

import (
	"math/rand"
	"time"
)


func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		rf.mu.Lock() // ------- 锁! -------
		if rf.state != Leader && time.Since(rf.lastTouchedAt) > SELECTION_TIMEOUT {

			
			rf.becomeCandidate()


			// 在持锁、term 刚 ++ 的这一刻就把 args 定死
			lastLogIndex := rf.logLength() - 1
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  rf.get(lastLogIndex).Term,
			}
			go rf.collectOpinion(args)
		}
		rf.mu.Unlock() // ------- 锁! -------


	
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		

	}
}



type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term int // candidate's term!
	CandidateId int

	// 2B:
	LastLogIndex int 
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {

	Term int
	VoteGranted bool

}


func (rf *Raft) collectOpinion(args *RequestVoteArgs) {

	
	supporter := 1 // 只在持 rf.mu 时访问,没问题
	


    for i := range rf.peers {

		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := &RequestVoteReply{}
			

			ok := rf.sendRequestVote(server, args, reply)// args 只读,可共享

			// ---------------- server 处理中！ ---------------

			if !ok {
				return 
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			
			if reply.Term > rf.currentTerm {
				rf.newGen(reply.Term)
			}

			if reply.VoteGranted && rf.state == Candidate && rf.currentTerm == args.Term {
				supporter++
				if supporter > len(rf.peers) / 2 {
					rf.becomeLeader()
				}
			}


		}(i)


	}


}




func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}



// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	

	if args.Term > rf.currentTerm {
		rf.newGen(args.Term)
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return 
		
	} 


	reply.VoteGranted = rf.tryVotingFor(args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	reply.Term = rf.currentTerm



	




}

