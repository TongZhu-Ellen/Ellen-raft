package raft 





// Make raft的时候就要打开！
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()

		for {
			// ① 先检查死没死
			if rf.killed() {
				rf.mu.Unlock()
				return
			}

			// ② 再检查有没有活
			if rf.lastApplied < rf.commitIndex {
				break // break出去干活！
			}

			// 没死，也没活 -> 睡
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































