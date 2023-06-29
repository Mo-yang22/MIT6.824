package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "time"
import "strings"
import "strconv"
import "io/ioutil"
import "sync"

// Task
const (
	MapTask      = 0
	ReduceTask   = 1
	WaitingTask  = 2
	TerminalTask = 3
)

type Task struct {
	TaskType    int
	InputFiles  []string
	TaskId      int
	ReduceId    int
	ReduceCount int
}

// TaskMetaHolder
const (
	Undo  = 0
	Doing = 1
	Done  = 2
)

type TaskMetaInfo struct {
	Condition int
	StartTime time.Time
	TaskPtr   *Task
}
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}

func (t *TaskMetaHolder) PutInfo(taskId int, taskInfo *TaskMetaInfo) bool {
	_, ok := t.MetaMap[taskId]
	if ok {
		fmt.Println("PutInfo error")
		return false
	}
	t.MetaMap[taskId] = taskInfo
	return true
}
func (t *TaskMetaHolder) Fire(taskId int) bool {
	elem, ok := t.MetaMap[taskId]
	if !ok || t.MetaMap[taskId].Condition == Undo {
		fmt.Println("Fire error")
		return false
	}
	elem.Condition = Undo
	elem.StartTime = time.Now()
	return true
}

const (
	MapPhase    = 0
	ReducePhase = 1
	DonePhase   = 2
)

type Coordinator struct {
	// Your definitions here.
	TaskChannelMap    chan *Task
	TaskChannelReduce chan *Task
	// MapNum            int
	ReduceNum int
	TaskCount int
	Metadata  TaskMetaHolder
	Phase     int
	Mutex     sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) TellTask(args *Args, reply *Task) error {
	c.Mutex.Lock()
	if c.Phase == MapPhase {
		select {
		// 还有MapTask没有被处理
		case data := <-c.TaskChannelMap:
			info, ok := c.Metadata.MetaMap[data.TaskId]
			if ok && info.Condition == Undo {
				info.StartTime = time.Now()
				info.Condition = Doing
				*reply = *data
			}
		default:
			reply.TaskType = WaitingTask
			reply.TaskId = 98
		}
	} else if c.Phase == ReducePhase {
		select {
		case data := <-c.TaskChannelReduce:
			info, ok := c.Metadata.MetaMap[data.TaskId]
			if ok && info.Condition == Undo {
				// 在交付任务的时候才计时?
				info.StartTime = time.Now()
				info.Condition = Doing
				*reply = *data
			}
		default:
			reply.TaskType = WaitingTask
			reply.TaskId = 99
		}
	} else if c.Phase == DonePhase {
		reply.TaskType = TerminalTask
	}
	c.Mutex.Unlock()
	return nil
}
func (c *Coordinator) TaskDone(args *Args, reply *Task) error {
	c.Mutex.Lock()
	taskId := args.TaskId
	info, ok := c.Metadata.MetaMap[taskId]
	if !ok {
		fmt.Printf("TaskDone Error %v\n", taskId)
		c.Mutex.Unlock()
		return nil
	}
	// if info.Condition != Doing {
	// 	fmt.Printf("Have Reduplicate %v\n", taskId)
	// 	c.Mutex.Unlock()
	// 	return nil
	// }
	info.Condition = Done
	c.Mutex.Unlock()
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
	c.Mutex.Lock()
	ret := false

	// Your code here.
	c.Check()
	if c.Phase == DonePhase {
		ret = true
	}
	c.Mutex.Unlock()
	return ret
}

// 在Done函数中周期性运行,看来判断是不是有task超时且看是否可以切换形态
func (c *Coordinator) Check() {
	var curMapNum int = 0
	var curReduceNum int = 0
	for taskId, info := range c.Metadata.MetaMap {
		timeout := 10 * time.Second
		deadline := info.StartTime.Add(timeout)
		if time.Now().After(deadline) && info.Condition == Doing {
			fmt.Printf("TaskId: %v超时了\n", taskId)
			c.Metadata.Fire(taskId)
			if info.TaskPtr.TaskType == MapTask {
				c.TaskChannelMap <- info.TaskPtr
			} else if info.TaskPtr.TaskType == ReduceTask {
				c.TaskChannelReduce <- info.TaskPtr
			}
		}
		if info.Condition != Done && info.TaskPtr.TaskType == MapTask {
			curMapNum++
		} else if info.Condition != Done && info.TaskPtr.TaskType == ReduceTask {
			curReduceNum++
		}
	}
	if c.Phase == MapPhase && curMapNum == 0 {
		c.Phase = ReducePhase
		c.MakeReduceTasks()
	} else if c.Phase == ReducePhase && curReduceNum == 0 {
		c.Phase = DonePhase
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		ReduceNum:         nReduce,
		TaskCount:         0,
		Metadata: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo),
		},
	}
	// Your code here.
	c.MakeMapTasks(files)
	c.server()
	return &c
}

func (c *Coordinator) GenerateTaskId() int {
	tmp := c.TaskCount
	c.TaskCount++
	return tmp
}
func (c *Coordinator) MakeMapTasks(files []string) {
	for _, file := range files {
		id := c.GenerateTaskId()
		fmt.Println("making map task: ", id)
		task := Task{
			TaskType:    MapTask,
			InputFiles:  []string{file},
			TaskId:      id,
			ReduceCount: c.ReduceNum,
			ReduceId:    0,
		}
		c.TaskChannelMap <- &task
		tmp_info := TaskMetaInfo{
			Condition: Undo,
			StartTime: time.Now(),
			TaskPtr:   &task,
		}
		c.Metadata.PutInfo(id, &tmp_info)
	}
	fmt.Println("done making map tasks")
}
func (c *Coordinator) MakeReduceTasks() {
	for i := 0; i < c.ReduceNum; i++ {
		id := c.GenerateTaskId()
		fmt.Println("making reduce task :", id)
		task := Task{
			TaskType:    ReduceTask,
			TaskId:      id,
			InputFiles:  TmpFileAssignHelper(i),
			ReduceCount: c.ReduceNum,
			ReduceId:    i,
		}
		tmp_info := TaskMetaInfo{
			Condition: Undo,
			StartTime: time.Now(),
			TaskPtr:   &task,
		}
		c.TaskChannelReduce <- &task
		c.Metadata.PutInfo(id, &tmp_info)

	}
	fmt.Println("done making reduce jobs")
}

// 函数的目的是在指定路径下查找以特定命名规则命名的文件，并将符合条件的文件名放入一个字符串切片中并返回。
func TmpFileAssignHelper(whichReduce int) []string {
	var res []string
	path, _ := os.Getwd()
	rd, _ := ioutil.ReadDir(path) //读取指定路径下的文件列表
	for _, fi := range rd {
		// 检查文件名是否以 "mr-tmp"开头并以指定的whichReduce数字结尾
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(whichReduce)) {
			res = append(res, fi.Name())
		}
	}
	return res
}
