package raft 





// Make raft的时候就要打开！
func (rf *Raft) applier() {
	for !rf.killed() {

		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		

		

		start := rf.lastApplied + 1
		end := rf.commitIndex

		applies := make([]ApplyMsg, 0, end+1-start)

		for i := start; i <= end; i++ {
			
			applies = append(applies, ApplyMsg{
				CommandValid: true,
				Command:      rf.get(i).Command,
				CommandIndex: i,
			})
		}

		rf.lastApplied = end
		rf.mu.Unlock()

		for _, apply := range applies {
			rf.applyCh <- apply
		}
	}
}
































