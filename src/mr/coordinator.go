package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"
// coordinator 是一个管理MapReduce任务的主节点
type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	files []string
	tasks []Task
	MapComplete bool
}

// Your code here -- RPC handlers for the worker to call.
// Task代表一个任务（map或者reduce任务）
type Task struct {
    Type string
	FileName string
	ID int
	Status string // 任务状态：空闲、进行中、完成"waiting"、"in-progress"、"completed"
	WorkerID int
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

	// Your code here.
	// 检查是否Map任务和Reduce任务都完成了
	for _,task := range c.tasks {
		if task.Status != "completed"{  //&& t.Type != "map" 
			return false
		}
	}
	ret = true
	// 如果所有任务都完成了，则返回true
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files

	// 创建map任务
	for i, file := range files {
		c.tasks = append(c.tasks, Task{Type: "map", 
		FileName: file, 
		ID: i,
		Status: "waiting",})
	}
	fmt.Println("Coordinator: ", c.tasks)
	// 启动RPC服务器
	c.server()
	return &c
}
