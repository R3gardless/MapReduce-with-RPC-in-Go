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

	// Create a buffered channel to hold task indicies
	tasks := make(chan int, ntasks)
	for i := 0; i < ntasks; i++ {
		tasks <- i // Fill the channel with task indices
	}

	// Continue scheduling tasks until every tasks are completed
	for len(tasks) > 0 {
		processTask(mr.jobName, phase, mr.files, mr.registerChannel, tasks, len(tasks), nios)
	}

	debug("Schedule: %v phase done\n", phase)
}

func processTask(jobName string, phase jobPhase, files []string, registerChannel chan string, tasks chan int, ntasks int, nios int) {
	nworkers := make(chan int) // Channel to track worker status
	for len(tasks) > 0 {       // Continue until all tasks are processed
		t := <-tasks // Get the next task from the channel
		var task DoTaskArgs
		switch phase {
		case mapPhase:
			task = DoTaskArgs{jobName, files[t], mapPhase, t, nios}
		case reducePhase:
			task = DoTaskArgs{jobName, "", reducePhase, t, nios}
		}

		// Launch a goroutine to handle the task
		go func(task DoTaskArgs) {
			address := <-registerChannel
			// DoTask is called by the master when a new task is being scheduled on this worker.
			// func (wk *Worker) DoTask(arg *DoTaskArgs, _ *struct{})

			// Schedule the task on the worker
			if call(address, "Worker.DoTask", task, nil) {
				nworkers <- 1
				registerChannel <- address
			} else {
				// log.Printf("Task %d failed on worker %s\n", task.TaskNumber, address) // Log the failure
				nworkers <- 0            // Task failed
				tasks <- task.TaskNumber // Requeue the task
			}
		}(task)
	}
	for p := 0; p < ntasks; p++ {
		<-nworkers
	}
}
