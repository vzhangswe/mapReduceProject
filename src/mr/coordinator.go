package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type status int // status of worker
const (
	ready status = iota
	running
	taskDone
)
const workerMaxTime = 12 * time.Second

type Coordinator struct {
	reduceCount      int // number of reduce task
	mapCount         int // number of map task
	taskDone         bool
	reduceTaskStatus []status
	mapTaskStatus    []status
	// match task with worker
	runningMapMatch    []RpcIdType
	runningReduceMatch []RpcIdType
	mapTasks      chan TaskIdType // map tasks that will be assigned
	reduceTasks   chan TaskIdType // reduce tasks that will be assigned
	files         []string     // task file
	mapCompleteCount        int          // Number of completed map tasks
	reduceCompleteCount     int          // Number of completed reduce tasks
	latch         *sync.Cond // lock
}

// Assign task to workers
func (coor *Coordinator) AssignTask(request *RequestArgs, reply *ResArgs) error {
	reply.ResponseId = request.RequestId
	reply.MapCount = coor.mapCount
	reply.ReduceCount = coor.reduceCount

	coor.latch.L.Lock()
	done := coor.taskDone
	coor.latch.L.Unlock()
	if done {
		reply.ResponseOp = DoneWork
		return nil
	}
	switch request.RequestOP {
	case WorkRequest:
		{
			// request a task
			coor.latch.L.Lock()
			if len(coor.mapTasks) > 0 {
				taskId := <-coor.mapTasks
				reply.ResponseTaskId = taskId
				reply.ResponseContent = coor.files[taskId]
				reply.ResponseOp = MapWork
				coor.runningMapMatch[taskId] = reply.ResponseId
				coor.mapTaskStatus[taskId] = running
				coor.latch.L.Unlock()
				go coor.checkDone(MapWork, reply.ResponseTaskId)
				log.Printf("Assign map \t%d to \t%d\n", reply.ResponseTaskId, reply.ResponseId)
				return nil
			}
			if coor.mapCompleteCount < coor.mapCount {
				// waiting for the map task to complete
				reply.ResponseOp = WaitWork
				coor.latch.L.Unlock()
				log.Println("Map All assigned but not done")
				return nil
			}
			if len(coor.reduceTasks) > 0 {
				// start to assign reduce task
				taskId := <-coor.reduceTasks
				reply.ResponseTaskId = taskId
				reply.ResponseOp = ReduceWork
				coor.runningReduceMatch[taskId] = reply.ResponseId
				coor.reduceTaskStatus[taskId] = running
				coor.latch.L.Unlock()
				go coor.checkDone(ReduceWork, reply.ResponseTaskId)
				log.Printf("Assign reduce \t%d to \t%d\n", reply.ResponseTaskId, reply.ResponseId)
				return nil
			}
			// waiting for the reduce tasks to complete
			reply.ResponseOp = WaitWork
			log.Println("Reduce All assigned but not done")
			coor.latch.L.Unlock()
			return nil
		}
	case MapDoneWork:
		{
			coor.latch.L.Lock()
			defer coor.latch.L.Unlock()
			if coor.runningMapMatch[request.RequestTaskId] != request.RequestId || coor.mapTaskStatus[request.RequestTaskId] != running {
				// the map task has terminated
				reply.ResponseOp = WorkTerminate
				return nil
			}
			log.Printf("Work Map \t%d done by \t%d\n", request.RequestTaskId, request.RequestId)
			coor.mapTaskStatus[request.RequestTaskId] = taskDone
			coor.mapCompleteCount++
		}
	case ReduceDoneWork:
		{
			coor.latch.L.Lock()
			defer coor.latch.L.Unlock()
			if coor.runningReduceMatch[request.RequestTaskId] != request.RequestId || coor.reduceTaskStatus[request.RequestTaskId] != running {
				// the reducec task has terminated
				reply.ResponseOp = WorkTerminate
				return nil
			}
			coor.reduceTaskStatus[request.RequestTaskId] = taskDone
			coor.reduceCompleteCount++
			log.Printf("Work Reduce \t%d done by \t%d\n", request.RequestTaskId, request.RequestId)
			if coor.reduceCompleteCount == coor.reduceCount {
				coor.taskDone = true
				reply.ResponseOp = DoneWork
			}
		}
	default:
		return nil
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (coor *Coordinator) server() {
	log.Println("Server Start")
	errInfo := rpc.Register(coor)
	if errInfo != nil {
		log.Fatal("register error:", errInfo)
	}
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	_ = os.Remove(sockname)

	l, errInfo := net.Listen("unix", sockname)
	go func(l net.Listener) {
		for {
			time.Sleep(5 * time.Second)
			if coor.Done() {
				errInfo := l.Close()
				if errInfo != nil {
					log.Fatal("close error:", errInfo)
				}
			}
		}
	}(l)

	if errInfo != nil {
		log.Fatal("listen error:", errInfo)
	}
	go func() {
		errInfo := http.Serve(l, nil)
		if errInfo != nil {
			log.Fatal("server error:", errInfo)
		}
	}()
}

// check if all tasks have completed
func (coor *Coordinator) Done() bool {
	coor.latch.L.Lock()
	defer coor.latch.L.Unlock()
	return coor.taskDone
}

// checkDone if task has completed
func (coor *Coordinator) checkDone(workType WorkType, t TaskIdType) {
	time.Sleep(workerMaxTime)
	coor.latch.L.Lock()
	defer coor.latch.L.Unlock()
	switch workType {
	case MapWork:
		{
			if coor.mapTaskStatus[t] != taskDone {
				coor.mapTaskStatus[t] = ready
				coor.mapTasks <- t
			}
		}
	case ReduceWork:
		{
			if coor.reduceTaskStatus[t] != taskDone {
				coor.reduceTaskStatus[t] = ready
				coor.reduceTasks <- t
			}
		}
	default:
		log.Panicf("Invalid WorkType %v\n", workType)
	}

}

// MakeCoordinator create a Coordinator.
// reduceCount is the number of reduce tasks to use.
func MakeCoordinator(files []string, reduceCount int) *Coordinator {
	log.Println("Launching Coordinator Factory")
	coor := Coordinator{}
	coor.reduceCount = reduceCount
	coor.mapCount = len(files) // each file is one map
	coor.taskDone = false

	coor.files = files

	coor.mapTasks = make(chan TaskIdType, coor.mapCount)
	coor.mapTaskStatus = make([]status, coor.mapCount)
	coor.runningMapMatch = make([]RpcIdType, coor.mapCount)
	coor.reduceTaskStatus = make([]status, reduceCount)
	coor.reduceTasks = make(chan TaskIdType, reduceCount)
	coor.runningReduceMatch = make([]RpcIdType, reduceCount)
	coor.latch = sync.NewCond(&sync.Mutex{})

	for i := 0; i < coor.mapCount; i++ {
		coor.mapTasks <- TaskIdType(i)
		coor.runningMapMatch[i] = -1
		coor.mapTaskStatus[i] = ready
	}
	for i := 0; i < coor.reduceCount; i++ {
		coor.reduceTasks <- TaskIdType(i)
		coor.runningReduceMatch[i] = -1
		coor.reduceTaskStatus[i] = ready
	}
	coor.server()
	return &coor
}
