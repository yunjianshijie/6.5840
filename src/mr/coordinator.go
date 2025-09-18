package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

// import "fmt"
import "time"

// coordinator 是一个管理MapReduce任务的主节点
type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	// reduce任务数
	nReduce int // reduce任务数量
	nMap    int // map任务数量

	files []string // 输入文件名

	//tasks []Task	// 任务列表

	mapTasks    []Task // map任务列表
	reduceTasks []Task // reduce任务列表

	phase JobPhase // 当前状态：map阶段还是reduce阶段

	MapComplete    int  // map阶段完成计数
	ReduceComplete int  // reduce阶段完成计数
	isDone         bool // 是否完成
}

// 任务的状态
type TaskStatus int

const (
	Idle       TaskStatus = iota // 空闲
	InProgress                   // 进行中
	Completed                    // 完成
)

// Your code here -- RPC handlers for the worker to call.
// Task代表一个任务（map或者reduce任务）
type Task struct {
	ID        int
	Type      string
	FileName  string     // map任务对应的文件名
	Status    TaskStatus // 任务状态
	WorkerID  int        // 任务分配给的工作节点ID
	StartTime time.Time  // 任务开始时间，用于检测超时
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	//
	// fmt.Println("AssignTask")
	switch c.phase {
	case MapPhase:
		// 找一个空闲的map任务
		for i := range c.mapTasks {
			if c.mapTasks[i].Status == Idle {
				// 如果空闲 ,先填写任务信息
				c.mapTasks[i].Status = InProgress
				c.mapTasks[i].StartTime = time.Now()
				// 填写worker信息
				reply.TaskType = MapTask                 // 任务类型
				reply.TaskId = c.mapTasks[i].ID          // 任务ID
				reply.InputFile = c.mapTasks[i].FileName // 输入文件
				reply.NReduce = c.nReduce                // reduce任务数量
				return nil
			}
		}
		// 分配完之后 worker等待
		reply.TaskType = WaitTask

	case ReducePhase:
		// 分配reduce任务
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == Idle {
				// 如果空闲
				c.reduceTasks[i].Status = InProgress
				c.reduceTasks[i].StartTime = time.Now()
				//
				reply.TaskType = ReduceTask
				reply.TaskId = c.reduceTasks[i].ID
				reply.NMap = c.nMap
				return nil
			}
		}
		reply.TaskType = WaitTask
	case DonePhase:
		// 全部完成
		reply.TaskType = ExitTask
	}
	return nil
}

func (c *Coordinator) ReportStatus(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	//
	//  fmt.Println("收到worker的回复 ,任务类型为:",args.TaskType)
	taskID := args.TaskId
	// 判断任务类型
	TaskType := args.TaskType
	switch TaskType {
	case MapTask:
		if c.mapTasks[taskID].Status == InProgress {
			c.MapComplete++
			c.mapTasks[taskID].Status = Completed
			if c.MapComplete == c.nMap {
				c.phase = ReducePhase // 进入reduce阶段
			}
		}
	case ReduceTask:
		if c.reduceTasks[taskID].Status == InProgress {
			c.ReduceComplete++
			c.reduceTasks[taskID].Status = Completed
			if c.ReduceComplete == c.nReduce {
				c.phase = DonePhase // 进入完成阶段
			}
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	c.mu.Lock()
	defer c.mu.Unlock()

	ret := false
	// map
	for _, task := range c.mapTasks {
		if task.Status != Completed {
			return false
		}
	}
	// reduce
	for _, task := range c.reduceTasks {
		if task.Status != Completed {
			return false
		}
	}

	ret = true
	// 如果所有任务都完成了，则返回true
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		isDone:      false,
		nMap:        len(files),
		nReduce:     nReduce,
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}
	// 创建map任务 (有几个文件就创建几个map任务)
	for i, file := range files {
		c.mapTasks[i] = Task{
			ID:       i,
			Status:   Idle,
			FileName: file,
		}
	}
	// 创建reduce任务 （初始化的nReduce ）
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			ID:     i,
			Status: Idle,
		}
	}

	// fmt.Println("Coordinator: ", c.mapTasks ," \n" ,c.reduceTasks)
	// 启动RPC服务器
	c.server()
	return &c
}
