package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

const sleepTime = 500 * time.Millisecond

// KeyValue
// Map functions return a slice of KeyValue
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// Sorting use the hashkey
func (a ByKey) Len() int{ 
	return len(a) 
}

func (a ByKey) Swap(i, j int) { 
	a[i], a[j] = a[j], a[i] 
}

func (a ByKey) Less(i, j int) bool {
	return ihash(a[i].Key) < ihash(a[j].Key) 
}


// use ihash(key) % reduceCount to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		return 0
	}
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
		for {
			currentTime := time.Now().Unix()
			rpcId := RpcIdType(currentTime)
			request := RequestArgs{}
			request.RequestId = rpcId
			request.RequestOP = WorkRequest // Request a work from coordinator
	
			response := ResArgs{}
			ok := call("Coordinator.AssignTask", &request, &response)
			if !ok {
				// Error Handling
				log.Println("Maybe Coordinator Server has been closed")
				return
			}
	
			switch response.ResponseOp {
				case MapWork:
					mapWork(rpcId, &response, mapf)
				case ReduceWork:
					doReduce(rpcId, &response, reducef)
				case WaitWork:
					// waiting
					time.Sleep(sleepTime)
				case DoneWork:
					return					
				default:
					break
			}
			time.Sleep(sleepTime)
		}
}

func mapWork(rpcId RpcIdType, res *ResArgs, mapf func(string, string) []KeyValue) {
	fileName := res.ResponseContent
	file, errInfo := os.Open(fileName)
	if errInfo != nil {
		log.Fatalf("Worker cannot open %v", fileName)
	}
	defer func(file *os.File) {
		file.Close()
	}(file)
	
	// read content
	content, errInfo := io.ReadAll(file)
	if errInfo != nil {
		log.Fatalf("worker cannot read %v", fileName)
	}
	kvs := mapf(fileName, string(content))

	// output kv pair to temp file
	outputFiles := make([]*os.File, res.ReduceCount)
	encodersArr := make([]*json.Encoder, res.ReduceCount)
	for i := 0; i < res.ReduceCount; i++ {
		// temp file name
		oname := "mr-" + strconv.Itoa(int(res.ResponseTaskId)) + "-" + strconv.Itoa(i)
		outputFiles[i], errInfo = os.Create(oname)
		if errInfo != nil {
			log.Fatal("Worker cannot create intermediate file: ", oname)
		}
		defer func(file *os.File, oname string) {
			errInfo := file.Close()
			if errInfo != nil {
				log.Fatal("Worker cannot close intermediate file", oname)
			}
		}(outputFiles[i], oname)
		encodersArr[i] = json.NewEncoder(outputFiles[i])
	}
	for _, kv := range kvs {
		ri := ihash(kv.Key) % res.ReduceCount
		errInfo := encodersArr[ri].Encode(kv)
		if errInfo != nil {
			log.Fatal("Encode Error: ", errInfo)
			return
		}
	}
	req := RequestArgs{
		RequestId:     rpcId,
		RequestOP:     MapDoneWork,
		RequestTaskId: res.ResponseTaskId,
	}
	response := ResArgs{}
	call("Coordinator.AssignTask", &req, &response)
}

func doReduce(rpcId RpcIdType, res *ResArgs, reducef func(string, []string) string) {
	tastId := res.ResponseTaskId
	var kvaPair []KeyValue
	for i := 0; i < res.MapCount; i++ {
		// read mid id
		func(mapId int) {
			// read tem file
			inputName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(int(tastId))
			// get all keys
			ifile, errInfo := os.Open(inputName)
			if errInfo != nil {
				log.Fatal("Worker cannot open file: ", inputName)
			}
			defer func(file *os.File) {
				errInfo := file.Close()
				if errInfo != nil {
					log.Fatal("Worker cannot close file: ", inputName)
				}
			}(ifile)
			dec := json.NewDecoder(ifile)
			for {
				var kvPair KeyValue
				if errInfo := dec.Decode(&kvPair); errInfo != nil {
					break
				}
				kvaPair = append(kvaPair, kvPair) //
			}
			os.Remove(inputName)
		}(i)
	}
	// sort by hashkey
	sort.Sort(ByKey(kvaPair))
	interPair := kvaPair[:]

	oname := "mr-out-" + strconv.Itoa(int(tastId))
	ofile, errInfo := os.Create(oname)
	if errInfo != nil {
		log.Fatal("Worker cannot create file: ", oname)
	}
	defer func(ofile *os.File) {
		errInfo := ofile.Close()
		if errInfo != nil {
			log.Fatal("Worker cannot close file: ", oname)
		}
	}(ofile)

	i := 0
	for i < len(interPair) {
		j := i + 1
		for j < len(interPair) && interPair[j].Key == interPair[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, interPair[k].Value)
		}
		output := reducef(interPair[i].Key, values)
		// print information
		_, fprintf := fmt.Fprintf(ofile, "%v %v\n", interPair[i].Key, output)
		if fprintf != nil {
			return
		}
		i = j
	}

	req := RequestArgs{
		RequestId:     rpcId,
		RequestOP:     ReduceDoneWork,
		RequestTaskId: res.ResponseTaskId,
	}
	response := ResArgs{}
	call("Coordinator.AssignTask", &req, &response)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcName string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, errInfo := rpc.DialHTTP("unix", sockname)
	if errInfo != nil {
		log.Fatal("dialing:", errInfo)
	}
	defer func(c *rpc.Client) {
		errInfo := c.Close()
		if errInfo != nil {
			log.Fatal("Close Client Error When RPC Calling", errInfo)
		}
	}(c)

	errInfo = c.Call(rpcName, args, reply)
	if errInfo == nil {
		return true
	}

	fmt.Println(errInfo)
	return false
}
