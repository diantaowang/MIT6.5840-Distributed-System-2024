package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
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

	// Your worker implementation here.
	for {
		reply, ok := CallGetJob()
		if !ok {
			// fmt.Println("worker get job failed. exit!")
			break
		} else if reply.Job == MapTask {
			filename := reply.FileName
			// fmt.Printf("get a map job: filename = %s, id = %d\n", reply.FileName, reply.MapID)
			domapf(filename, reply.MapID, reply.NReduce, mapf)
			ok = CallMapFinish(reply.MapID)
			if !ok {
				fmt.Println("worker send map finish signal failed. exit!")
				break
			}
		} else if reply.Job == ReduceTask {
			// fmt.Printf("get a reduce job-%d\n", reply.ReduceID)
			doreducef(reply.ReduceID, reply.NMap, reducef)
			ok = CallReduceFinish(reply.ReduceID)
			if !ok {
				fmt.Println("worker send reduce finish signal failed. exit!")
				break
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func domapf(filename string, mapID int, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file %v", filename)
	}
	defer file.Close()

	kva := mapf(filename, string(content))

	var pwd string
	pwd, err = os.Getwd()
	if err != nil {
		log.Fatalf("get pwd failed")
	}

	tmpFiles := make([](*os.File), nReduce)
	for i := 0; i < nReduce; i++ {
		fname := "mr-" + strconv.Itoa(mapID) + "-" + strconv.Itoa(i) + "_*.txt.tmp"
		tmpFiles[i], err = os.CreateTemp(pwd, fname)
		if err != nil {
			log.Fatalf("create tmp file %v failed", fname)
		}
	}

	for _, kv := range kva {
		hashid := ihash(kv.Key) % nReduce
		fmt.Fprintf(tmpFiles[hashid], "%v %v\n", kv.Key, kv.Value)
	}

	for i := 0; i < nReduce; i++ {
		defer tmpFiles[i].Close()
		oldname := tmpFiles[i].Name()
		newname := renameTmpFile(oldname, ".txt.tmp")
		// fmt.Printf("%s, %s\n", oldname, newname)
		err = os.Rename(oldname, newname)
		if err != nil {
			log.Fatalf("rename tmpfile name failed")
		}
	}
}

func doreducef(reduceID int, nMap int, reducef func(string, []string) string) {
	var kva []KeyValue
	for i := 0; i < nMap; i++ {
		fname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceID) + ".txt.tmp"

		file, err := os.Open(fname)
		if err != nil {
			log.Fatalf("cannot open %v", fname)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			words := strings.Split(line, " ")
			if len(words) != 2 {
				log.Fatal("split key and value failed")
			}
			kva = append(kva, KeyValue{words[0], words[1]})
		}
	}
	sort.Sort(ByKey(kva))

	pattern := "mr-out-" + strconv.Itoa(reduceID) + "-*"
	ofile, err := os.CreateTemp(".", pattern)
	if err != nil {
		log.Fatalf("create tmp file %v failed", ofile.Name())
	}
	defer ofile.Close()
	// fmt.Printf("%s\n", ofile.Name())

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

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	newname := renameTmpFile(ofile.Name(), "")
	err = os.Rename(ofile.Name(), newname)
	if err != nil {
		log.Fatalf("rename tmpfile name failed")
	}
}

func renameTmpFile(oldname string, suffix string) string {
	n := len(oldname)
	m := len(suffix)
	if m >= n {
		log.Fatalf("not a suffix")
	}
	end := n - m - 1
	for ; end >= 0 && oldname[end] >= '0' && oldname[end] <= '9'; end-- {
	}
	return oldname[:end] + suffix
}

func CallGetJob() (*GetJobReply, bool) {
	args := ExampleArgs{}
	reply := GetJobReply{}
	ok := call("Coordinator.GetJob", &args, &reply)
	return &reply, ok
}

func CallMapFinish(mapID int) bool {
	args := ExampleArgs{}
	reply := ExampleReply{}
	args.X = mapID
	return call("Coordinator.MapFinish", &args, &reply)
}

func CallReduceFinish(reduceID int) bool {
	args := ExampleArgs{}
	reply := ExampleReply{}
	args.X = reduceID
	return call("Coordinator.ReduceFinish", &args, &reply)
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
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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

	fmt.Println(err)
	return false
}
