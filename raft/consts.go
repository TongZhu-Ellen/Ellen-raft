package raft 

import "time"



const (
	SELECTION_TIMEOUT = 500 * time.Millisecond
	HEATBEAT_INTERVAL =  100 * time.Millisecond
	
)








type RaftState int
const (
    Follower  RaftState = iota // 0
    Candidate                   // 1
    Leader                      // 2
)