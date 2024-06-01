package mapreduce

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	tasks := make(chan int, ntasks)
	for i := 0; i < ntasks; i++ {
		tasks <- i
	}
	for len(tasks) > 0 {
		processTask(mr.jobName, phase, mr.files, mr.registerChannel, tasks, len(tasks), nios)
	}

	debug("Schedule: %v phase done\n", phase)
}

func processTask(jobName string, phase jobPhase, mapFiles []string, registerChan chan string, tasks chan int, ntasks int, n_other int) {
	nworkers := make(chan int)
	for len(tasks) > 0 {
		t := <-tasks
		var task DoTaskArgs
		switch phase {
		case mapPhase:
			task = DoTaskArgs{jobName, mapFiles[t], mapPhase, t, n_other}
		case reducePhase:
			task = DoTaskArgs{jobName, "", reducePhase, t, n_other}
		}
		go func(task DoTaskArgs) {
			address := <-registerChan
			// DoTask is called by the master when a new task is being scheduled on this worker.
			// func (wk *Worker) DoTask(arg *DoTaskArgs, _ *struct{})
			if call(address, "Worker.DoTask", task, nil) {
				nworkers <- 1
				registerChan <- address
			} else {
				nworkers <- 0
				tasks <- task.TaskNumber
			}
		}(task)
	}
	for p := 0; p < ntasks; p++ {
		<-nworkers
	}
}
