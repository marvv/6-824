package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

//type DoTaskArgs struct {
//	JobName    string
//	File       string   // only for map, the input file
//	Phase      jobPhase // are we in mapPhase or reducePhase?
//	TaskNumber int      // this task's index in the current phase
//
//	// NumOtherPhase is the total number of tasks in other phase; mappers
//	// need this to compute the number of output bins, and reducers needs
//	// this to know how many input files to collect.
//	NumOtherPhase int
//}

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	wg := sync.WaitGroup{}
	taskIndex := 0

	for addr := range registerChan {
		wg.Add(1)
		args := DoTaskArgs{
			JobName:jobName,
			Phase:phase,
			TaskNumber:taskIndex,
			NumOtherPhase:n_other,
		}
		if phase == mapPhase {
			args.File = mapFiles[taskIndex]
		}
		taskIndex++
		go func(svr string,arg DoTaskArgs) {
			for !call(svr, "Worker.DoTask", &arg, nil) {
				svr = <- registerChan
			}
			wg.Done()
			registerChan <- svr
		} (addr, args)
		if taskIndex >= ntasks {
			break
		}
	}


	wg.Wait()
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
