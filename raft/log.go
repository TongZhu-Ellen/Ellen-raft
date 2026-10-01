package raft  

/*
public util about log! 
NO LOCK. 
*/
















func (rf *Raft) get(i int) Entry {
    return rf.log[i]
}

func (rf *Raft) entriesFrom(start int) []Entry {
    return append([]Entry{}, rf.log[start:]...)
}

func (rf *Raft) logLength() int { // this is the real length of all the things happened before. nothing about truancating. 
    return len(rf.log)
}
















func (rf *Raft) append(entry Entry) {

	rf.log = append(rf.log, entry)
	rf.persist()
}

func (rf *Raft) batchAppend(entries []Entry) {
    rf.log = append(rf.log, entries...)
    rf.persist()
}