package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv"
import "sync"
import "time"

type Coordinator struct {
	Started bool
	StartedReduce bool
	Finished bool
	MapIdleTasks []Task
	ReduceIdleTasks []Task
	ProcessingTasks []Task
	mu sync.Mutex
}

type Task struct {
	Number int
	Filename string
	Type string
	NReduce int
	Finished bool
}

// return a task to worker
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	DPrintf("[Coordinator] Worker requesting task")
	// get first task and assign it to worker
	task := Task{}
	reply.Continue = true

	c.mu.Lock()
	if c.Started {
		if len(c.MapIdleTasks) > 0 {
			task = c.MapIdleTasks[0]
			reply.T = task
			// Remove task from idle tasks
			c.MapIdleTasks = c.MapIdleTasks[1:]
			c.ProcessingTasks = append(c.ProcessingTasks, task)
			go c.CheckTaskFinished(task)
		} else {
			if len(c.MapIdleTasks) == 0 && len(c.ProcessingTasks) == 0 {
				c.StartedReduce = true
			}
			if c.StartedReduce == true && len(c.ReduceIdleTasks) > 0 {
				task = c.ReduceIdleTasks[0]
				reply.T = task
				// Remove task from idle tasks
				c.ReduceIdleTasks = c.ReduceIdleTasks[1:]
				c.ProcessingTasks = append(c.ProcessingTasks, task)
				go c.CheckTaskFinished(task)
			}
		}
	}
	if c.Started == true && len(c.MapIdleTasks) == 0 && len(c.ReduceIdleTasks) == 0 && len(c.ProcessingTasks) == 0 {
		reply.Continue = false
	}
	c.mu.Unlock()

	return nil
}

// finish task
func (c *Coordinator) FinishTask(task *Task, reply *bool) error {
	// get task and mark it as finished
	DPrintf("[Coordinator] Received finish signal for task " + task.Type + " " + strconv.Itoa(task.Number))
	c.mu.Lock()
	for i, t := range c.ProcessingTasks {
		if t.Type == task.Type && t.Number == task.Number && t.Finished == false {
			// removing task from processing queue
			c.ProcessingTasks[i] = c.ProcessingTasks[len(c.ProcessingTasks)-1]
    		c.ProcessingTasks = c.ProcessingTasks[:len(c.ProcessingTasks)-1]
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) CheckTaskFinished(task Task) {
	// Check if task has finished after a certain time
	// If task does not finish, put it back in idle tasks queue
	time.Sleep(10 * time.Second)

	deleteTaskIndex := -1
	c.mu.Lock()
	for i, t := range c.ProcessingTasks {
		if t.Type == task.Type && t.Number == task.Number {
			if t.Finished == false {
				deleteTaskIndex = i
			}
		}
	}

	// Remove task from processing task list
	// and put it back in idle task list
	if deleteTaskIndex >= 0 {
		DPrintf("[Coordinator] Stopping task " + strconv.Itoa(task.Number))
		c.ProcessingTasks[deleteTaskIndex] = c.ProcessingTasks[len(c.ProcessingTasks)-1]
    	c.ProcessingTasks = c.ProcessingTasks[:len(c.ProcessingTasks)-1]
		if task.Type == "MAP" {
			c.MapIdleTasks = append(c.MapIdleTasks, task)
		} else if task.Type == "REDUCE" {
			c.ReduceIdleTasks = append(c.ReduceIdleTasks, task)
		}
	}
	c.mu.Unlock()
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	c.mu.Lock()
	if c.Started && len(c.MapIdleTasks) == 0 && len(c.ReduceIdleTasks) == 0 && len(c.ProcessingTasks) == 0 {
		DPrintf("[Coordinator] Job done. Exiting...")
		return true
	}
	c.mu.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// this function will create idle map and reduce tasks
// and start the server to listen to workers
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	DPrintf("[Coordinator] Starting coordinator...")

	for i, filename := range files {
		task := Task{}
		task.Type = "MAP"
		task.Filename = filename
		task.Number = i
		task.NReduce = nReduce
		task.Finished = false
		c.MapIdleTasks = append(c.MapIdleTasks, task)
	}
	DPrintf("[Coordinator] " + strconv.Itoa(len(c.MapIdleTasks)) + " map tasks created")

	for i := 0; i < nReduce; i++ {
		task := Task{}
		task.Type = "REDUCE"
		task.Filename = "mr-*-" + strconv.Itoa(i)
		task.Number = i
		task.Finished = false
		c.ReduceIdleTasks = append(c.ReduceIdleTasks, task)
	}
	DPrintf("[Coordinator] " + strconv.Itoa(len(c.ReduceIdleTasks)) + " reduce tasks created")
	
	if len(c.MapIdleTasks) > 0 {
		c.Started = true
	}
	c.StartedReduce = false
	c.Finished = false

	DPrintf("[Coordinator] Listening to workers...")

	c.server()
	return &c
}
