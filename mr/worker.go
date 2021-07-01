package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

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

	reply := GetWorkWrapper(&GetWorkArgs{Data: false})
	for !reply.Quit {
		if reply.Job == Map {
			// Map all the files in parallel
			ch := make(chan []KeyValue, len(reply.Files))
			for _, fname := range reply.Files {
				go func(filename string) {
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open %v", filename)
					}
					content, err := ioutil.ReadAll(file)
					if err != nil {
						log.Fatalf("cannot read %v", filename)
					}
					file.Close()
					kva := mapf(filename, string(content))
					ch <- kva
				}(fname)
			}
			// Collect reducer mapping
			reducerData := make(map[int][]KeyValue)
			for i := 0; i < len(reply.Files); i++ {
				dat := <-ch
				for _, kv := range dat {
					reducerData[ihash(kv.Key)%reply.NumReduce] = append(reducerData[ihash(kv.Key)%reply.NumReduce], kv)
				}
			}
			// fmt.Printf("Came out of collecting channels, %v", reducerData)
			// Write out data to temporary file
			var wg sync.WaitGroup
			files := make([]string, 0)
			for i, kva := range reducerData {
				wg.Add(1)
				fname := fmt.Sprintf("mr-temp-%v-%v", reply.TaskNo, i)
				files = append(files, fname)
				go func(filename string, kva []KeyValue, wg *sync.WaitGroup) {
					defer wg.Done()
					file, err := ioutil.TempFile("", filename)
					if err != nil {
						fmt.Printf("Error while opening %v\n", filename)
						return
					}
					enc := json.NewEncoder(file)
					for _, kv := range kva {
						// fmt.Printf("Enoding %v\n", kv)
						err := enc.Encode(kv)
						// fmt.Fprintf(file, "%v", kv)
						if err != nil {
							fmt.Printf("Encoding failed for %v\n", kv)
						}
					}
					err = file.Close()
					if err != nil {
						fmt.Printf("Error while closing %v\n", err)
					}
					os.Rename(file.Name(), filename)
				}(fname, kva, &wg)
			}
			wg.Wait()
			// Send details to co-ordinator
			reply = GetWorkWrapper(&GetWorkArgs{Data: true, Job: Map, TaskNo: reply.TaskNo, Files: files})
		} else if reply.Job == Reduce {
			oname := fmt.Sprintf("mr-out-%v", reply.ReduceNo)
			intermediate := make([]KeyValue, 0)
			for _, filename := range reply.Files {
				file, err := os.Open(filename)
				if err != nil {
					fmt.Printf("Error while opening %v\n", filename)
					return
				}
				defer file.Close()
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))
			ofile, err := ioutil.TempFile("", oname)
			// fmt.Println(intermediate)
			if err != nil {
				fmt.Printf("Error while opening temp file %v\n", oname)
			}

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-X.
			//
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()
			os.Rename(ofile.Name(), oname)
			reply = GetWorkWrapper(&GetWorkArgs{Data: true, Job: Reduce, ReduceNo: reply.ReduceNo})
		} else {
			// Wait as no job available
			time.Sleep(5 * time.Second)
			reply = GetWorkWrapper(&GetWorkArgs{Data: false})
		}
	}

}

func GetWorkWrapper(args *GetWorkArgs) *GetWorkReply {
	reply := GetWorkReply{}
	if !call("Coordinator.GetWork", args, &reply) {
		return &GetWorkReply{Quit: true}
	}
	return &reply
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
