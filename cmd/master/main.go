package main

import (
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/semmidev/map-reduce/internal/common"
	"github.com/semmidev/map-reduce/internal/master"
)

func main() {
	port := getEnv("MASTER_PORT", "8080")
	addr := ":" + port

	inputDir := getEnv("INPUT_DIR", "/data")
	outputDir := getEnv("OUTPUT_DIR", "/output")
	nReduceStr := getEnv("N_REDUCE", "3")
	nReduce := 3
	if n, err := parseInt(nReduceStr); err == nil {
		nReduce = n
	}

	// Discover input files from the mounted data directory
	inputFiles, err := filepath.Glob(filepath.Join(inputDir, "*.log"))
	if err != nil || len(inputFiles) == 0 {
		log.Fatalf("[MASTER] No input files found in %s", inputDir)
	}
	log.Printf("[MASTER] Found %d input files: %v", len(inputFiles), inputFiles)

	config := common.JobConfig{
		Name:        "cyber-attack-log-analysis",
		InputFiles:  inputFiles,
		NReduce:     nReduce,
		OutputDir:   outputDir,
		JobType:     "cyberlog_analysis",
	}

	m := master.New(config)
	m.Start(addr)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseInt(s string) (int, error) {
	var n int
	_, err := parseIntHelper(s, &n)
	return n, err
}

func parseIntHelper(s string, n *int) (int, error) {
	s = strings.TrimSpace(s)
	result := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, &parseError{s}
		}
		result = result*10 + int(c-'0')
	}
	*n = result
	return result, nil
}

type parseError struct{ s string }
func (e *parseError) Error() string { return "cannot parse: " + e.s }
