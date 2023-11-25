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

type RpcIdType int64 // RpcIdType RpcId
type WorkType int
type TaskIdType int

type RequestArgs struct {
	RequestId     RpcIdType
	RequestOP     WorkType
	RequestTaskId TaskIdType
}

// ResArgs return from RPC
// Response
type ResArgs struct {
	ResponseId      RpcIdType
	ResponseOp      WorkType
	ResponseTaskId  TaskIdType // Task ID
	ResponseContent string
	ReduceCount int // the number of reduce task
	MapCount    int // the number of map task
}

// Task Type
const (
	WaitWork    WorkType = iota
	WorkRequest                 // worker request to work
	MapWork                 // assign map work to worker
	ReduceWork              // assign reduce work to worker
	DoneWork                // all work are done
	WorkTerminate           // stop working
	MapDoneWork             // worker done the map work
	ReduceDoneWork          // worker finish the reduce work
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
