package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"
import "io/ioutil"
import "encoding/json"
import "sort"
import "strconv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// Len 方法返回切片的長度。
func (a ByKey) Len() int {
	return len(a)
}

// Swap 方法交換切片中兩個元素的位置。
func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// Less 方法定義了排序的規則：如果第 i 個元素的 Key
// 在字母序上小於第 j 個元素的 Key，則返回 true。
func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// fmt.Println("Worker is running")
	//无限循环，不断向coordinator请求任务
	for {
		// 请求任务
		task := requestTask()
		// 根据任务类型执行
		switch task.TaskType {
		case MapTask: // coordinator向worker分配Map Task
			// fmt.Println("MapTask")
			HandleMapTask(task, mapf)
		case ReduceTask: // 分配Reduce Task
			// fmt.Println("ReduceTask")
			HandleReduceTask(task, reducef)
		case WaitTask: // 等待
			// fmt.Println("WaitTask")
			time.Sleep(time.Second * 10)
		case ExitTask: // 终止
			// fmt.Println("ExitTask")
			os.Exit(0)
		}
	}
}

// 请求任务
func requestTask() *TaskReply {

	args := TaskArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)

	if !ok {
		// 如果 RPC 调用失败，意味着 coordinator 可能已经崩溃
		// worker 无法继续工作，直接退出
		// // fmt.Println("requestTask_fail")
		os.Exit(0)
	}
	return &reply
}

// 如果分配到map任务
func HandleMapTask(reply *TaskReply, mapf func(string, string) []KeyValue) error {
	// fmt.Println("HandleMapTask")
	// 读文件
	file, err := os.Open(reply.InputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	// 执行map函数
	kva := mapf(reply.InputFile, string(content))
	sort.Sort(ByKey(kva))

	// 桶
	tempFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)

	// 写入分区
	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % reply.NReduce
		if encoders[reduceIndex] == nil {
			// intermediateFiles[reduceIndex] = append(intermediateFiles[reduceIndex], kv)
			tempFile, err := ioutil.TempFile(".", fmt.Sprintf("mr-map-temp-%d", reduceIndex))
			if err != nil {
				return err
			}
			defer tempFile.Close()
			tempFiles[reduceIndex] = tempFile
			encoders[reduceIndex] = json.NewEncoder(tempFile)
		}
		err := encoders[reduceIndex].Encode(&kv)
		if err != nil {
			return err
		}
		// // fmt.Println(kv)
	}
	for i, file := range tempFiles {

		if file != nil {

			fileName := file.Name()
			newName := fmt.Sprintf("mr-out-%d-%d", reply.TaskId, i)
			err := os.Rename(fileName, newName)
			if err != nil {
				// fmt.Println(err)
				return err
			}
		}
	}
	// fmt.Println("CallReportStatus")
	// 告诉coordinator已经完成了任务
	CallReportStatus(reply.TaskId, reply.TaskType)
	return nil
}

// 如果分配到reduce任务
func HandleReduceTask(reply *TaskReply, reducef func(string, []string) string) error {
	// fmt.Println("HandleReduceTask")
	// // fmt.Println(reply)
	reduceID := reply.TaskId
	var kva []KeyValue
	// 读取所有的map输出

	for i := 0; i < reply.NMap; i++ {
		fileName := fmt.Sprintf("mr-out-%d-%d", i, reduceID)
		file, err := os.Open(fileName)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			// // fmt.Println(kv)
			kva = append(kva, kv)
		}
		file.Close()
	}
	// 2.排序
	sort.Sort(ByKey(kva))
	// 3.
	// outputFileName := fmt.Sprintf("mr-out-%d", reduceID)
	keys := []string{}
	k_vs := make(map[string][]string) // 使用make初始化
	for _, kv := range kva {
		keys = append(keys, kv.Key)
		k_vs[kv.Key] = append(k_vs[kv.Key], kv.Value)
	}
	sort.Strings(keys)
	oname := "mr-out-" + strconv.Itoa(reply.TaskId) // + "-" + strconv.Itoa(reply.TaskId)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	for _, key := range keys {
		output := reducef(key, k_vs[key])
		_, err := fmt.Fprintf(ofile, "%s %s\n", key, output)
		if err != nil {
			return err
		}
	}
	CallReportStatus(reply.TaskId, reply.TaskType)
	return nil
}

// 告诉coordinator已经完成了任务
func CallReportStatus(taskId int, TaskType TaskType) {
	args := TaskArgs{
		TaskId:   taskId,
		TaskType: TaskType,
	}
	reply := TaskReply{}
	call("Coordinator.ReportStatus", &args, &reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
	// 发送RPC请求，等待回复。
	// "Coordinator.Example"告知接收服务器，
	// 我们希望调用结构体Coordinator的Example()方法。
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		// fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		// fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	// fmt.Println(err)
	return false
}
