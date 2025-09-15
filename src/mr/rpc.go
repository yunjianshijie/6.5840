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
	MsgType MsgType // 消息类型
	TsakId int 		// 任务ID
	TaskName string // 任务名称
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
