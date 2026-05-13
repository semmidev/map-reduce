package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/semmidev/map-reduce/internal/common"
	"github.com/semmidev/map-reduce/jobs"
)

const (
	pollInterval      = 1 * time.Second
	heartbeatInterval = 5 * time.Second
)

// MapFunc is the signature for user-defined map functions
type MapFunc func(filename, content string) []common.KeyValue

// ReduceFunc is the signature for user-defined reduce functions
type ReduceFunc func(key string, values []string) string

// Worker polls the master for tasks and executes them
type Worker struct {
	id         string
	masterAddr string
	mapFn      jobs.MapFunc
	reduceFn   jobs.ReduceFunc
	httpClient *http.Client
}

// New creates a Worker connected to a master
func New(id, masterAddr string, mapFn jobs.MapFunc, reduceFn jobs.ReduceFunc) *Worker {
	return &Worker{
		id:         id,
		masterAddr: masterAddr,
		mapFn:      mapFn,
		reduceFn:   reduceFn,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// Start registers with the master and begins the work loop
func (w *Worker) Start(workerAddr string) {
	log.Printf("[WORKER %s] Starting, connecting to master at %s", w.id, w.masterAddr)

	// Keep trying to register until master is reachable
	for {
		if err := w.register(workerAddr); err != nil {
			log.Printf("[WORKER %s] Registration failed: %v. Retrying...", w.id, err)
			time.Sleep(2 * time.Second)
			continue
		}
		break
	}

	log.Printf("[WORKER %s] Registered successfully", w.id)

	// Start heartbeat goroutine
	go w.heartbeatLoop()

	// Main work loop
	w.workLoop()
}

func (w *Worker) register(addr string) error {
	req := common.RegisterWorkerRequest{
		WorkerID: w.id,
		Address:  addr,
	}
	var resp common.RegisterWorkerResponse
	if err := w.post("/register", req, &resp); err != nil {
		return err
	}
	if !resp.Accepted {
		return fmt.Errorf("rejected: %s", resp.Message)
	}
	return nil
}

func (w *Worker) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for range ticker.C {
		var resp common.HeartbeatResponse
		_ = w.post("/heartbeat", common.HeartbeatRequest{WorkerID: w.id}, &resp)
	}
}

func (w *Worker) workLoop() {
	for {
		var resp common.GetTaskResponse
		if err := w.post("/get-task", common.GetTaskRequest{WorkerID: w.id}, &resp); err != nil {
			log.Printf("[WORKER %s] Error getting task: %v", w.id, err)
			time.Sleep(pollInterval)
			continue
		}

		task := resp.Task
		switch task.Type {
		case common.TaskTypeDone:
			log.Printf("[WORKER %s] 🎉 Job complete. Shutting down.", w.id)
			return

		case common.TaskTypeWait:
			time.Sleep(pollInterval)

		case common.TaskTypeMap:
			w.executeMap(task)

		case common.TaskTypeReduce:
			w.executeReduce(task)
		}
	}
}

// executeMap runs the user's map function on an input file
func (w *Worker) executeMap(task common.Task) {
	log.Printf("[WORKER %s] Starting MAP task %d: %v", w.id, task.ID, task.InputFiles)
	start := time.Now()

	result := common.TaskResult{
		TaskID:   task.ID,
		TaskType: common.TaskTypeMap,
		WorkerID: w.id,
	}

	content, err := os.ReadFile(task.InputFiles[0])
	if err != nil {
		result.Status = common.TaskStatusFailed
		result.Error = fmt.Sprintf("read file: %v", err)
		w.reportTask(result)
		return
	}

	// Run user map function
	kvs := w.mapFn(task.InputFiles[0], string(content))
	log.Printf("[WORKER %s] MAP task %d produced %d key-value pairs", w.id, task.ID, len(kvs))

	// Partition by reduce bucket and write intermediate files
	buckets := make(map[int][]common.KeyValue)
	for _, kv := range kvs {
		bucket := ihash(kv.Key) % task.NReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	var outputFiles []string
	for reduceID, pairs := range buckets {
		fname := fmt.Sprintf("/tmp/mr-map-%d-%d", task.ID, reduceID)
		f, err := os.Create(fname)
		if err != nil {
			result.Status = common.TaskStatusFailed
			result.Error = fmt.Sprintf("create intermediate file: %v", err)
			w.reportTask(result)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range pairs {
			enc.Encode(kv)
		}
		f.Close()
		outputFiles = append(outputFiles, fname)
	}

	result.Status = common.TaskStatusCompleted
	result.OutputFiles = outputFiles
	result.Duration = time.Since(start).Seconds()
	log.Printf("[WORKER %s] MAP task %d done in %.2fs → %d intermediate files",
		w.id, task.ID, result.Duration, len(outputFiles))
	w.reportTask(result)
}

// executeReduce runs the user's reduce function across intermediate files
func (w *Worker) executeReduce(task common.Task) {
	log.Printf("[WORKER %s] Starting REDUCE task %d: %d input files", w.id, task.ID, len(task.InputFiles))
	start := time.Now()

	result := common.TaskResult{
		TaskID:   task.ID,
		TaskType: common.TaskTypeReduce,
		WorkerID: w.id,
	}

	// Collect all key-value pairs from all intermediate files
	var kvs []common.KeyValue
	for _, fname := range task.InputFiles {
		f, err := os.Open(fname)
		if err != nil {
			log.Printf("[WORKER %s] Warning: can't open %s: %v", w.id, fname, err)
			continue
		}
		dec := json.NewDecoder(f)
		for {
			var kv common.KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		f.Close()
	}

	// Sort by key (canonical MapReduce behaviour)
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })

	// Write final output
	outPath := fmt.Sprintf("/output/mr-out-%d", task.ReduceID)
	outFile, err := os.Create(outPath)
	if err != nil {
		result.Status = common.TaskStatusFailed
		result.Error = fmt.Sprintf("create output file: %v", err)
		w.reportTask(result)
		return
	}
	defer outFile.Close()

	// Group by key and call reduce
	i := 0
	outputLines := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		out := w.reduceFn(kvs[i].Key, values)
		fmt.Fprintf(outFile, "%s\t%s\n", kvs[i].Key, out)
		outputLines++
		i = j
	}

	result.Status = common.TaskStatusCompleted
	result.OutputFiles = []string{outPath}
	result.Duration = time.Since(start).Seconds()
	log.Printf("[WORKER %s] REDUCE task %d done in %.2fs → %d unique keys → %s",
		w.id, task.ID, result.Duration, outputLines, outPath)
	w.reportTask(result)
}

func (w *Worker) reportTask(result common.TaskResult) {
	var resp common.ReportTaskResponse
	if err := w.post("/report-task", common.ReportTaskRequest{Result: result}, &resp); err != nil {
		log.Printf("[WORKER %s] Failed to report task %d: %v", w.id, result.TaskID, err)
	}
}

func (w *Worker) post(path string, body, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	resp, err := w.httpClient.Post(w.masterAddr+path, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("post %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server error: %s", resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// ihash distributes keys evenly across reduce buckets
func ihash(key string) int {
	h := 0
	for _, c := range key {
		h = h*31 + int(c)
	}
	if h < 0 {
		h = -h
	}
	return h
}
