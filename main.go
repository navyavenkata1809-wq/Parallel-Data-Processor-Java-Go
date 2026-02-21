package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Task represents the unit of work
type Task struct {
	ID      int
	Payload string
}

// Result represents the outcome of a task
type Result struct {
	WorkerID int
	TaskID   int
	Message  string
	Err      error
}

// Worker function that processes tasks from a channel
func worker(id int, tasks <-chan Task, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Printf("Worker %d starting...\n", id)

	for task := range tasks {
		fmt.Printf("Worker %d processing task %d\n", id, task.ID)

		// Simulate computational work
		processingTime := time.Duration(200+rand.Intn(500)) * time.Millisecond
		time.Sleep(processingTime)

		// Simulate potential error
		var err error
		if rand.Intn(10) == 0 {
			err = fmt.Errorf("simulated processing error for task %d", task.ID)
		}

		res := Result{
			WorkerID: id,
			TaskID:   task.ID,
			Message:  fmt.Sprintf("Worker %d finished task %d", id, task.ID),
			Err:      err,
		}
		results <- res
	}

	fmt.Printf("Worker %d completing.\n", id)
}

// ResultLogger handles writing to a file (shared resource)
func resultLogger(results <-chan Result, done chan<- bool) {
	file, err := os.OpenFile("results_go.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		return
	}
	defer file.Close()

	for res := range results {
		if res.Err != nil {
			fmt.Printf("[ERROR] %v\n", res.Err)
			continue
		}

		logLine := fmt.Sprintf("%s | Timestamp: %v\n", res.Message, time.Now().Unix())
		if _, err := file.WriteString(logLine); err != nil {
			fmt.Printf("Error writing to file: %v\n", err)
		}
	}
	done <- true
}

func main() {
	rand.Seed(time.Now().UnixNano())

	const numWorkers = 4
	const totalTasks = 20

	// Channels act as the concurrency-safe queue and communication lines
	taskQueue := make(chan Task, 10)
	results := make(chan Result, 10)
	loggerDone := make(chan bool)

	var wg sync.WaitGroup

	// 1. Start Logger (Shared Resource Manager)
	go resultLogger(results, loggerDone)

	// 2. Start Worker Pool
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, taskQueue, results, &wg)
	}

	// 3. Producer: Send tasks to the queue
	go func() {
		for i := 1; i <= totalTasks; i++ {
			taskQueue <- Task{ID: i, Payload: fmt.Sprintf("Data-%d", i)}
		}
		close(taskQueue) // Signal workers there are no more tasks
	}()

	// 4. Concurrency Management: Wait for workers to finish
	wg.Wait()
	close(results) // Signal logger there are no more results

	// 5. Safe Termination: Wait for logger to finish disk I/O
	<-loggerDone

	fmt.Println("System execution finished.")
}
