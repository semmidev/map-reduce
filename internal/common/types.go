package common

import "time"

// TaskType defines the type of MapReduce task
type TaskType string

const (
	TaskTypeMap    TaskType = "MAP"
	TaskTypeReduce TaskType = "REDUCE"
	TaskTypeWait   TaskType = "WAIT"
	TaskTypeDone   TaskType = "DONE"
)

// TaskStatus represents the current state of a task
type TaskStatus string

const (
	TaskStatusIdle       TaskStatus = "IDLE"
	TaskStatusInProgress TaskStatus = "IN_PROGRESS"
	TaskStatusCompleted  TaskStatus = "COMPLETED"
	TaskStatusFailed     TaskStatus = "FAILED"
)

// Task represents a unit of work assigned to a worker
type Task struct {
	ID         int       `json:"id"`
	Type       TaskType  `json:"type"`
	InputFiles []string  `json:"input_files"`
	ReduceID   int       `json:"reduce_id"`
	NReduce    int       `json:"n_reduce"`
	AssignedAt time.Time `json:"assigned_at"`
}

// TaskResult is what a worker reports back to the master
type TaskResult struct {
	TaskID      int        `json:"task_id"`
	TaskType    TaskType   `json:"task_type"`
	WorkerID    string     `json:"worker_id"`
	OutputFiles []string   `json:"output_files"`
	Status      TaskStatus `json:"status"`
	Error       string     `json:"error,omitempty"`
	Duration    float64    `json:"duration_seconds"`
}

// WorkerInfo holds metadata about a registered worker
type WorkerInfo struct {
	ID           string     `json:"id"`
	Address      string     `json:"address"`
	Status       string     `json:"status"`
	CurrentTask  *Task      `json:"current_task,omitempty"`
	TasksHandled int        `json:"tasks_handled"`
	RegisteredAt time.Time  `json:"registered_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

// JobConfig holds the configuration for a MapReduce job
type JobConfig struct {
	Name        string   `json:"name"`
	InputFiles  []string `json:"input_files"`
	NReduce     int      `json:"n_reduce"`
	OutputDir   string   `json:"output_dir"`
	JobType     string   `json:"job_type"` // e.g. "cyberlog_analysis"
}

// JobStatus represents the overall state of the MapReduce job
type JobStatus struct {
	Phase           string      `json:"phase"` // "MAP", "REDUCE", "DONE"
	TotalMapTasks   int         `json:"total_map_tasks"`
	DoneMapTasks    int         `json:"done_map_tasks"`
	TotalReduceTasks int        `json:"total_reduce_tasks"`
	DoneReduceTasks int         `json:"done_reduce_tasks"`
	Workers         []WorkerInfo `json:"workers"`
	StartTime       time.Time   `json:"start_time"`
	EndTime         *time.Time  `json:"end_time,omitempty"`
}

// KeyValue is the fundamental unit emitted by Map and consumed by Reduce
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// RPC Request/Response types

// RegisterWorkerRequest is sent by worker on startup
type RegisterWorkerRequest struct {
	WorkerID string `json:"worker_id"`
	Address  string `json:"address"`
}

// RegisterWorkerResponse is sent back to confirm registration
type RegisterWorkerResponse struct {
	Accepted bool   `json:"accepted"`
	Message  string `json:"message"`
}

// GetTaskRequest is sent by worker to request a task
type GetTaskRequest struct {
	WorkerID string `json:"worker_id"`
}

// GetTaskResponse is the task assigned (or WAIT/DONE signal)
type GetTaskResponse struct {
	Task Task `json:"task"`
}

// ReportTaskRequest is sent by worker after completing a task
type ReportTaskRequest struct {
	Result TaskResult `json:"result"`
}

// ReportTaskResponse acknowledges the result
type ReportTaskResponse struct {
	Acknowledged bool `json:"acknowledged"`
}

// HeartbeatRequest is sent periodically by workers
type HeartbeatRequest struct {
	WorkerID string `json:"worker_id"`
}

// HeartbeatResponse is the master's reply
type HeartbeatResponse struct {
	OK bool `json:"ok"`
}
