package master

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/semmidev/map-reduce/internal/common"
)

const (
	taskTimeout        = 30 * time.Second
	heartbeatTimeout   = 15 * time.Second
	heartbeatCheckFreq = 5 * time.Second
)

// Master orchestrates the distributed MapReduce job
type Master struct {
	mu sync.Mutex

	config common.JobConfig

	// Task tracking
	mapTasks    []taskEntry
	reduceTasks []taskEntry

	// Phase management
	phase string // "MAP" | "REDUCE" | "DONE"

	// Worker registry
	workers map[string]*common.WorkerInfo

	// Intermediate files produced by map tasks
	// intermediates[reduceID] = list of files
	intermediates map[int][]string

	done chan struct{}
}

type taskEntry struct {
	task       common.Task
	status     common.TaskStatus
	assignedTo string
	assignedAt time.Time
}

// New creates and initializes a Master
func New(config common.JobConfig) *Master {
	m := &Master{
		config:        config,
		phase:         "MAP",
		workers:       make(map[string]*common.WorkerInfo),
		intermediates: make(map[int][]string),
		done:          make(chan struct{}),
	}

	// Create map tasks - one per input file
	for i, f := range config.InputFiles {
		m.mapTasks = append(m.mapTasks, taskEntry{
			task: common.Task{
				ID:         i,
				Type:       common.TaskTypeMap,
				InputFiles: []string{f},
				NReduce:    config.NReduce,
			},
			status: common.TaskStatusIdle,
		})
	}

	// Create reduce tasks - one per reduce bucket
	for i := 0; i < config.NReduce; i++ {
		m.reduceTasks = append(m.reduceTasks, taskEntry{
			task: common.Task{
				ID:       i,
				Type:     common.TaskTypeReduce,
				ReduceID: i,
				NReduce:  config.NReduce,
			},
			status: common.TaskStatusIdle,
		})
	}

	// Ensure output directory exists
	_ = os.MkdirAll(config.OutputDir, 0755)

	log.Printf("[MASTER] Initialized: %d map tasks, %d reduce tasks", len(m.mapTasks), config.NReduce)

	return m
}

// Start begins the HTTP server for RPC communication
func (m *Master) Start(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/register", m.handleRegister)
	mux.HandleFunc("/get-task", m.handleGetTask)
	mux.HandleFunc("/report-task", m.handleReportTask)
	mux.HandleFunc("/heartbeat", m.handleHeartbeat)
	mux.HandleFunc("/status", m.handleStatus)

	go m.watchdog()

	log.Printf("[MASTER] Listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("[MASTER] Server error: %v", err)
	}
}

// Done returns true when the entire job is complete
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.phase == "DONE"
}

// ---- HTTP Handlers ----

func (m *Master) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req common.RegisterWorkerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	m.workers[req.WorkerID] = &common.WorkerInfo{
		ID:            req.WorkerID,
		Address:       req.Address,
		Status:        "IDLE",
		RegisteredAt:  time.Now(),
		LastHeartbeat: time.Now(),
	}
	m.mu.Unlock()

	log.Printf("[MASTER] Worker registered: %s @ %s", req.WorkerID, req.Address)

	respond(w, common.RegisterWorkerResponse{Accepted: true, Message: "Welcome to the cluster!"})
}

func (m *Master) handleGetTask(w http.ResponseWriter, r *http.Request) {
	var req common.GetTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	task := m.assignTask(req.WorkerID)
	respond(w, common.GetTaskResponse{Task: task})
}

func (m *Master) handleReportTask(w http.ResponseWriter, r *http.Request) {
	var req common.ReportTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	m.processResult(req.Result)
	respond(w, common.ReportTaskResponse{Acknowledged: true})
}

func (m *Master) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req common.HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	if w, ok := m.workers[req.WorkerID]; ok {
		w.LastHeartbeat = time.Now()
	}
	m.mu.Unlock()

	respond(w, common.HeartbeatResponse{OK: true})
}

func (m *Master) handleStatus(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	doneMap := 0
	for _, t := range m.mapTasks {
		if t.status == common.TaskStatusCompleted {
			doneMap++
		}
	}
	doneReduce := 0
	for _, t := range m.reduceTasks {
		if t.status == common.TaskStatusCompleted {
			doneReduce++
		}
	}

	workers := make([]common.WorkerInfo, 0, len(m.workers))
	for _, w := range m.workers {
		workers = append(workers, *w)
	}

	respond(w, common.JobStatus{
		Phase:            m.phase,
		TotalMapTasks:    len(m.mapTasks),
		DoneMapTasks:     doneMap,
		TotalReduceTasks: len(m.reduceTasks),
		DoneReduceTasks:  doneReduce,
		Workers:          workers,
	})
}

// ---- Core Logic ----

func (m *Master) assignTask(workerID string) common.Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch m.phase {
	case "MAP":
		for i := range m.mapTasks {
			t := &m.mapTasks[i]
			if t.status == common.TaskStatusIdle {
				t.status = common.TaskStatusInProgress
				t.assignedTo = workerID
				t.assignedAt = time.Now()
				if w, ok := m.workers[workerID]; ok {
					w.Status = "BUSY"
					taskCopy := t.task
					w.CurrentTask = &taskCopy
				}
				log.Printf("[MASTER] Assigned MAP task %d -> worker %s (file: %s)",
					t.task.ID, workerID, t.task.InputFiles[0])
				return t.task
			}
		}
		// All map tasks assigned or done - check if we should wait
		if !m.allMapsDone() {
			return common.Task{Type: common.TaskTypeWait}
		}

	case "REDUCE":
		for i := range m.reduceTasks {
			t := &m.reduceTasks[i]
			if t.status == common.TaskStatusIdle {
				t.status = common.TaskStatusInProgress
				t.assignedTo = workerID
				t.assignedAt = time.Now()
				// Attach the intermediate files for this reduce bucket
				t.task.InputFiles = m.intermediates[t.task.ReduceID]
				if w, ok := m.workers[workerID]; ok {
					w.Status = "BUSY"
					taskCopy := t.task
					w.CurrentTask = &taskCopy
				}
				log.Printf("[MASTER] Assigned REDUCE task %d -> worker %s (%d input files)",
					t.task.ID, workerID, len(t.task.InputFiles))
				return t.task
			}
		}
		if !m.allReducesDone() {
			return common.Task{Type: common.TaskTypeWait}
		}
	}

	return common.Task{Type: common.TaskTypeDone}
}

func (m *Master) processResult(result common.TaskResult) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if result.Status == common.TaskStatusFailed {
		log.Printf("[MASTER] Task %d FAILED by worker %s: %s", result.TaskID, result.WorkerID, result.Error)
		m.resetTask(result.TaskType, result.TaskID)
		return
	}

	log.Printf("[MASTER] Task %d COMPLETED by worker %s in %.2fs",
		result.TaskID, result.WorkerID, result.Duration)

	if result.TaskType == common.TaskTypeMap {
		for i := range m.mapTasks {
			if m.mapTasks[i].task.ID == result.TaskID {
				m.mapTasks[i].status = common.TaskStatusCompleted
				break
			}
		}
		// Collect intermediate files per reduce bucket
		for _, f := range result.OutputFiles {
			reduceID := extractReduceID(f)
			m.intermediates[reduceID] = append(m.intermediates[reduceID], f)
		}
		if w, ok := m.workers[result.WorkerID]; ok {
			w.Status = "IDLE"
			w.CurrentTask = nil
			w.TasksHandled++
		}
		if m.allMapsDone() {
			log.Printf("[MASTER] ✅ All MAP tasks complete. Transitioning to REDUCE phase.")
			m.phase = "REDUCE"
		}
	} else if result.TaskType == common.TaskTypeReduce {
		for i := range m.reduceTasks {
			if m.reduceTasks[i].task.ID == result.TaskID {
				m.reduceTasks[i].status = common.TaskStatusCompleted
				break
			}
		}
		if w, ok := m.workers[result.WorkerID]; ok {
			w.Status = "IDLE"
			w.CurrentTask = nil
			w.TasksHandled++
		}
		if m.allReducesDone() {
			log.Printf("[MASTER] 🎉 All REDUCE tasks complete. Job DONE!")
			m.phase = "DONE"
			close(m.done)
		}
	}
}

func (m *Master) resetTask(taskType common.TaskType, taskID int) {
	if taskType == common.TaskTypeMap {
		for i := range m.mapTasks {
			if m.mapTasks[i].task.ID == taskID {
				m.mapTasks[i].status = common.TaskStatusIdle
				m.mapTasks[i].assignedTo = ""
				break
			}
		}
	} else {
		for i := range m.reduceTasks {
			if m.reduceTasks[i].task.ID == taskID {
				m.reduceTasks[i].status = common.TaskStatusIdle
				m.reduceTasks[i].assignedTo = ""
				break
			}
		}
	}
}

func (m *Master) allMapsDone() bool {
	for _, t := range m.mapTasks {
		if t.status != common.TaskStatusCompleted {
			return false
		}
	}
	return true
}

func (m *Master) allReducesDone() bool {
	for _, t := range m.reduceTasks {
		if t.status != common.TaskStatusCompleted {
			return false
		}
	}
	return true
}

// watchdog detects timed-out tasks and re-queues them
func (m *Master) watchdog() {
	ticker := time.NewTicker(heartbeatCheckFreq)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.mu.Lock()
			now := time.Now()

			// Requeue stale map tasks
			for i := range m.mapTasks {
				t := &m.mapTasks[i]
				if t.status == common.TaskStatusInProgress && now.Sub(t.assignedAt) > taskTimeout {
					log.Printf("[MASTER] ⏰ MAP task %d timed out (worker %s), re-queueing", t.task.ID, t.assignedTo)
					t.status = common.TaskStatusIdle
					t.assignedTo = ""
				}
			}

			// Requeue stale reduce tasks
			for i := range m.reduceTasks {
				t := &m.reduceTasks[i]
				if t.status == common.TaskStatusInProgress && now.Sub(t.assignedAt) > taskTimeout {
					log.Printf("[MASTER] ⏰ REDUCE task %d timed out (worker %s), re-queueing", t.task.ID, t.assignedTo)
					t.status = common.TaskStatusIdle
					t.assignedTo = ""
				}
			}

			// Check for dead workers
			for id, w := range m.workers {
				if now.Sub(w.LastHeartbeat) > heartbeatTimeout {
					log.Printf("[MASTER] 💀 Worker %s heartbeat timeout, marking dead", id)
					w.Status = "DEAD"
				}
			}

			m.mu.Unlock()

		case <-m.done:
			return
		}
	}
}

// ---- Helpers ----

func respond(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func extractReduceID(filename string) int {
	// Files are named: mr-map-<mapID>-<reduceID>
	base := filepath.Base(filename)
	var mapID, reduceID int
	fmt.Sscanf(base, "mr-map-%d-%d", &mapID, &reduceID)
	return reduceID
}
