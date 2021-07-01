package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type JobType int

const (
	Map JobType = iota
	Reduce
	Wait
)


type GetWorkArgs struct {
	Data     bool
	Job      JobType
	TaskNo   int
	ReduceNo int
	Files    []string
}

type GetWorkReply struct {
	Job       JobType
	Files     []string
	TaskNo    int
	ReduceNo  int
	Quit      bool
	NumReduce int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
