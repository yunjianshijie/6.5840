package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type MsgType int // 用于区分消息类型
const (
	AskForTask    MsgType = iota // 表示worker向coordinator申请任务
	MapSucceed                   // 表示worker向coordinator传递Map Task完成
	MapFailed                    // 表示worker向coordinator传递Map Task失败
	ReduceSucceed                // 表示worker向coordinator传递Reduce Task完成
	ReduceFailed                 // 表示worker向coordinator传递Reduce Task失败
	MapAlloc                     // 表示coordinator向worker分配Map Task
	ReduceAlloc                  // 表示coordinator向worker分配Reduce Task
	Wait                         // 表示coordinator让worker休眠
	Shutdown                     // 表示coordinator让worker终止
)

type TaskType int
const (
    MapTask TaskType = iota
    ReduceTask
    WaitTask // 让 worker 等待的任务
    ExitTask // 让 worker 退出的任务
)

// 作业的阶段
type JobPhase int
const (
	MapPhase JobPhase = iota
	ReducePhase
	DonePhase
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}
// 定义消息类型 
type TaskArgs struct {
	MsgType MsgType
	TaskId int
}

type TaskReply struct {
	TaskType     TaskType // 任务类型
	TaskId 		 int 	  // 任务ID
	InputFile    string   // Map 任务的输入文件名
    NReduce      int      // Reduce 任务的数量 (Map 任务需要)
    NMap         int      // Map 任务的数量 (Reduce 任务需要)
    ReduceTaskID int      // Reduce 任务的编号 (Reduce 任务需要)
}

type MsgReply struct {
	MsgType MsgType // 消息类型
	TaskId int 		// 任务ID
	TaskName string // 任务名称
	FileName string // 文件名
	
	NumReduce int // reduce任务数量
	NumMap int // map任务数量
}


// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
// 在/var/tmp目录下为协调器生成一个较为独特的UNIX域套接字名称。
// 由于Athena AFS不支持UNIX域套接字，无法使用当前目录。
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
