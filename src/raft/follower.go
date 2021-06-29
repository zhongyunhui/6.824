package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) toCandidate() {
	rf.lock()
	defer rf.unLock()
	rf.myState = CandidateState
	rf.votedFor = rf.me
	//DPrintf("2A   节点%d变成Candidate", rf.me)
}


func (rf *Raft) checkFollower() {
	for !rf.killed() {
		rf.initTimerReset()


		timeout := time.Duration(300+rand.Int31n(150))*time.Millisecond
		for time.Since(rf.getTimerReset()) <= timeout && rf.getMyState() == FollowerState {
			time.Sleep(time.Millisecond)
		}
		rf.toCandidate()
		return

		//
		//if !rf.checkTimerReset(FollowerState) {
		//	rf.toCandidate()
		//	return
		//}
	}
}
