package mr

import "fmt"
import "log"
import "time"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "strconv"
import "encoding/json"
import "path/filepath"
import "sort"
import "github.com/google/uuid"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Turn on and get a task from coordinator
	// Continue until coordinator says so
	continueLoop := true
	reply := GetTaskReply{}
	uid := uuid.New().String()[:5]

	for continueLoop {
		DPrintf("[Worker %s] Requesting task...", uid)
		reply = GetTask()
		continueLoop = reply.Continue
		task := reply.T
		if task.Filename != "" {
			DPrintf("[Worker %s] Received task " + task.Type + " " + strconv.Itoa(task.Number) + " with file " + task.Filename, uid)
			if task.Type == "MAP" {
				RunMap(task, mapf)
				DPrintf("[Worker %s] Map task finished", uid)
			}
			if task.Type == "REDUCE" {
				RunReduce(task, reducef)
				DPrintf("[Worker %s] Reduce task finished", uid)
			}
			// Sleep before requesting another task
			time.Sleep(100 * time.Millisecond)
		}
	}
	DPrintf("[Worker %s] No more tasks. Exiting...", uid)
}

// Ask Coordinator for a task
func GetTask() (GetTaskReply) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	_ = ok
	return reply
}

// Tell Coordinator that a task is finished
func FinishTask(task Task) {
	reply := true

	ok := call("Coordinator.FinishTask", &task, &reply)
	if ok {
		// DO NOTHING
	} else {
		fmt.Printf("call failed!\n")
	}
}

func RunMap(task Task, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	kva := mapf(task.Filename, string(content))

	// Spreading keys
	buckets := make(map[int][]KeyValue)
	for _, kv := range kva {
		output := ihash(kv.Key) % task.NReduce

		buckets[output] = append(buckets[output], kv)
	}

	// Creating files
	for o, bucket := range buckets {
		oname := "mr-" + strconv.Itoa(task.Number) + "-" + strconv.Itoa(o)
		file, _ := os.Create(oname)

		enc := json.NewEncoder(file)
		for _, kv2 := range bucket {
		  err := enc.Encode(&kv2)
		  _ = err
		}
	}

	// Telling coordinator that task is finished
	FinishTask(task)
}

func RunReduce(task Task, reducef func(string, []string) string) {
	// Fetching key from all files
	kva := []KeyValue{}
	filenames, _ := filepath.Glob(task.Filename)

	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	//
	// call Reduce on each distinct key,
	// and print the result to file.
	//
	oname := "mr-out-" + strconv.Itoa(task.Number)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()

	// Telling coordinator that task is finished
	FinishTask(task)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}
