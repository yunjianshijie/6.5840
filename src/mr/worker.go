package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
		fmt.Println("Worker is running")
		//无限循环，不断向coordinator请求任务
		for{
			// args := TaskArgs{}
			reply := TaskReply{} // 接收任务信息 
			// ok := call("Coordinator.Example", &args, &reply) // 向coordinator请求任务

			switch reply.MsgType {
				
			case MapAlloc: 	    // coordinator向worker分配Map Task
				 fmt.Println("MapTask")
				 err := HandleMapTask(reply, mapf)
			case ReduceAlloc:   // 分配Reduce Task
				 fmt.Println("ReduceTask")
				 err := HandleReduceTask(reply, reducef)

			case Wait:			// 等待
				// fmt.Println("Wait")
				time.Sleep(time.Second * 10)
			case Shutdown: 		// 终止
				// fmt.Println("Shutdown")
				os.Exit(0)
			}
			// if ok {
			// // reply.Y should be 100.
			// 	fmt.Printf("reply.Y %v %v\n", reply.MsgType, reply)
			// } else {
			// 	fmt.Printf("call failed!\n")
			// 	return
			// }
		}
	

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.


	// declare an argument structure.

}

// 如果分配到map任务
func HandleMapTask(reply * MsgReply, mapf func(string, string) []KeyValue) error {
     fmt.Println("HandleRMapTask")
	// fmt.Println(reply)
	
}


// 如果分配到reduce任务
func HandleReduceTask(reply * MsgReply, reducef func(string, []string) string) error {
	 fmt.Println("HandleReduceTask")
	// fmt.Println(reply)

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
