package main

import (
	"fmt"
	"log"
	"os"

	"github.com/semmidev/map-reduce/internal/worker"
	"github.com/semmidev/map-reduce/jobs"
)

func main() {
	// Worker ID is set via env (from docker-compose hostname or explicit override)
	workerID := getEnv("WORKER_ID", mustHostname())

	// Master address - using Docker service name resolution
	masterHost := getEnv("MASTER_HOST", "master")
	masterPort := getEnv("MASTER_PORT", "8080")
	masterAddr := fmt.Sprintf("http://%s:%s", masterHost, masterPort)

	// Worker's own address (for master to identify it in logs)
	workerAddr := getEnv("WORKER_ADDR", workerID+":9090")

	log.Printf("[WORKER %s] Master: %s | Self: %s", workerID, masterAddr, workerAddr)

	w := worker.New(workerID, masterAddr, jobs.CyberLogMap, jobs.CyberLogReduce)
	w.Start(workerAddr)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func mustHostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown-worker"
	}
	return h
}
