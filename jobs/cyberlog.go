package jobs

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/semmidev/map-reduce/internal/common"
)

// MapFunc is the user-provided map function signature
type MapFunc func(filename, content string) []common.KeyValue

// ReduceFunc is the user-provided reduce function signature
type ReduceFunc func(key string, values []string) string

// ============================================================
// 🔐 USE CASE: Cyber Attack Log Analysis
// ============================================================
//
// We process millions of raw nginx/server access logs across
// multiple machines to detect:
//   - Top attacker IPs (volume-based detection)
//   - Attack pattern types (SQL injection, XSS, path traversal)
//   - Most targeted endpoints
//   - Geo-bucket anomalies (unusual country traffic spikes)
//   - Suspicious user-agent fingerprints
//
// Each log line looks like:
//   192.168.1.1 - - [13/May/2026:10:23:44 +0700] "GET /admin/../etc/passwd HTTP/1.1" 404 512 "-" "sqlmap/1.7"
//
// The Map phase tags each log entry with attack signals.
// The Reduce phase aggregates counts per threat category.
// ============================================================

// LogEntry represents a parsed access log line
type LogEntry struct {
	IP        string `json:"ip"`
	Timestamp string `json:"timestamp"`
	Method    string `json:"method"`
	Path      string `json:"path"`
	Status    int    `json:"status"`
	Bytes     int    `json:"bytes"`
	UserAgent string `json:"user_agent"`
}

// CyberLogMap parses server logs and emits threat signals
// Emits keys like:
//
//	"ATTACKER_IP:192.168.1.1"
//	"ATTACK_TYPE:SQL_INJECTION"
//	"TARGET_ENDPOINT:/admin/login"
//	"SUSPICIOUS_UA:sqlmap/1.7"
//	"STATUS_FLOOD:404"
func CyberLogMap(filename, content string) []common.KeyValue {
	var kvs []common.KeyValue

	lines := strings.Split(content, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Try JSON-structured log first, fall back to raw parsing
		entry := parseLine(line)
		if entry == nil {
			continue
		}

		// --- Signal 1: Track attacker IP volume ---
		kvs = append(kvs, common.KeyValue{
			Key:   fmt.Sprintf("ATTACKER_IP:%s", entry.IP),
			Value: "1",
		})

		// --- Signal 2: Detect attack patterns in path ---
		attackType := detectAttackType(entry.Path, entry.UserAgent)
		if attackType != "" {
			kvs = append(kvs, common.KeyValue{
				Key:   fmt.Sprintf("ATTACK_TYPE:%s", attackType),
				Value: entry.IP,
			})
			// Also link IP to specific attack type
			kvs = append(kvs, common.KeyValue{
				Key:   fmt.Sprintf("IP_ATTACK:%s:%s", entry.IP, attackType),
				Value: "1",
			})
		}

		// --- Signal 3: Track targeted endpoints ---
		// Normalize paths (strip query strings)
		cleanPath := normalizeEndpoint(entry.Path)
		kvs = append(kvs, common.KeyValue{
			Key:   fmt.Sprintf("TARGET_ENDPOINT:%s", cleanPath),
			Value: "1",
		})

		// --- Signal 4: Suspicious user agents ---
		if ua := detectSuspiciousUA(entry.UserAgent); ua != "" {
			kvs = append(kvs, common.KeyValue{
				Key:   fmt.Sprintf("SUSPICIOUS_UA:%s", ua),
				Value: entry.IP,
			})
		}

		// --- Signal 5: Error status flood detection ---
		if entry.Status >= 400 {
			kvs = append(kvs, common.KeyValue{
				Key:   fmt.Sprintf("STATUS_FLOOD:%d", entry.Status),
				Value: entry.IP,
			})
		}

		// --- Signal 6: High-frequency scan detection ---
		// Tracks requests that look like automated scanning
		if isScanning(entry.Path) {
			kvs = append(kvs, common.KeyValue{
				Key:   fmt.Sprintf("SCANNER_IP:%s", entry.IP),
				Value: entry.Path,
			})
		}
	}

	return kvs
}

// CyberLogReduce aggregates all signals per threat category
func CyberLogReduce(key string, values []string) string {
	parts := strings.SplitN(key, ":", 2)
	if len(parts) < 2 {
		return strconv.Itoa(len(values))
	}

	category := parts[0]

	switch category {
	case "ATTACKER_IP":
		// Sum request counts
		total := sumInts(values)
		severity := classifySeverity(total)
		return fmt.Sprintf(`{"requests":%d,"severity":"%s"}`, total, severity)

	case "ATTACK_TYPE":
		// Count unique IPs involved
		unique := uniqueCount(values)
		total := len(values)
		return fmt.Sprintf(`{"total_hits":%d,"unique_ips":%d}`, total, unique)

	case "IP_ATTACK":
		total := sumInts(values)
		return fmt.Sprintf(`{"count":%d}`, total)

	case "TARGET_ENDPOINT":
		total := sumInts(values)
		risk := classifyEndpointRisk(parts[1])
		return fmt.Sprintf(`{"hits":%d,"risk_level":"%s"}`, total, risk)

	case "SUSPICIOUS_UA":
		unique := uniqueCount(values)
		total := len(values)
		return fmt.Sprintf(`{"total_requests":%d,"unique_sources":%d}`, total, unique)

	case "STATUS_FLOOD":
		unique := uniqueCount(values)
		return fmt.Sprintf(`{"total_errors":%d,"unique_ips":%d}`, len(values), unique)

	case "SCANNER_IP":
		paths := uniquePaths(values)
		return fmt.Sprintf(`{"unique_paths_scanned":%d,"sample_paths":%s}`, len(paths), toJSON(paths[:min(5, len(paths))]))
	}

	return strconv.Itoa(len(values))
}

// ---- Parsing Helpers ----

func parseLine(line string) *LogEntry {
	// Try JSON first (structured log)
	if strings.HasPrefix(line, "{") {
		var e LogEntry
		if err := json.Unmarshal([]byte(line), &e); err == nil {
			return &e
		}
	}

	// Parse Combined Log Format:
	// IP - - [timestamp] "METHOD path HTTP/x.x" status bytes "-" "ua"
	parts := strings.Fields(line)
	if len(parts) < 7 {
		return nil
	}

	entry := &LogEntry{IP: parts[0]}

	// Find quoted request part
	reqStart := strings.Index(line, `"`)
	reqEnd := strings.Index(line[reqStart+1:], `"`) + reqStart + 1
	if reqStart >= 0 && reqEnd > reqStart {
		req := line[reqStart+1 : reqEnd]
		reqParts := strings.Fields(req)
		if len(reqParts) >= 2 {
			entry.Method = reqParts[0]
			entry.Path = reqParts[1]
		}
	}

	// Status code
	for i, p := range parts {
		if i > 5 {
			if status, err := strconv.Atoi(p); err == nil && status >= 100 && status < 600 {
				entry.Status = status
				break
			}
		}
	}

	// User agent (last quoted string)
	lastQuote := strings.LastIndex(line, `"`)
	if lastQuote > 0 {
		prevQuote := strings.LastIndex(line[:lastQuote], `"`)
		if prevQuote >= 0 && prevQuote < lastQuote {
			entry.UserAgent = line[prevQuote+1 : lastQuote]
		}
	}

	if entry.Path == "" {
		return nil
	}
	return entry
}

// ---- Attack Detection ----

var sqlPatterns = []string{
	"union+select", "union select", "' or '1'='1", "1=1", "or 1=1",
	"drop+table", "insert+into", "select+from", "xp_cmdshell",
	"../", "..%2f", "%27", "0x", "char(", "benchmark(",
	"information_schema", "sleep(", "waitfor+delay",
}

var xssPatterns = []string{
	"<script", "%3cscript", "javascript:", "onerror=", "onload=",
	"alert(", "document.cookie", "eval(", "fromcharcode",
}

var pathTraversalPatterns = []string{
	"../", "..\\", "%2e%2e", "....//", "etc/passwd",
	"etc/shadow", "win.ini", "boot.ini", "/proc/self",
}

var scanPaths = []string{
	".env", ".git", "wp-admin", "phpMyAdmin", "admin.php",
	"config.php", "backup", ".DS_Store", "Makefile",
	"actuator/", "api/swagger", ".well-known",
}

func detectAttackType(path, ua string) string {
	pathLower := strings.ToLower(path)
	uaLower := strings.ToLower(ua)
	combined := pathLower + " " + uaLower

	for _, p := range sqlPatterns {
		if strings.Contains(combined, p) {
			return "SQL_INJECTION"
		}
	}
	for _, p := range xssPatterns {
		if strings.Contains(combined, p) {
			return "XSS"
		}
	}
	for _, p := range pathTraversalPatterns {
		if strings.Contains(pathLower, p) {
			return "PATH_TRAVERSAL"
		}
	}
	if strings.Contains(uaLower, "masscan") || strings.Contains(uaLower, "nmap") ||
		strings.Contains(uaLower, "nikto") || strings.Contains(uaLower, "dirbuster") {
		return "ACTIVE_SCANNER"
	}
	return ""
}

func detectSuspiciousUA(ua string) string {
	uaLower := strings.ToLower(ua)
	suspiciousTools := []string{
		"sqlmap", "nikto", "masscan", "nmap", "dirbuster",
		"burpsuite", "curl/", "python-requests", "go-http-client",
		"zgrab", "shodan", "censys",
	}
	for _, tool := range suspiciousTools {
		if strings.Contains(uaLower, tool) {
			return tool
		}
	}
	return ""
}

func isScanning(path string) bool {
	pathLower := strings.ToLower(path)
	for _, p := range scanPaths {
		if strings.Contains(pathLower, p) {
			return true
		}
	}
	return false
}

func normalizeEndpoint(path string) string {
	if idx := strings.Index(path, "?"); idx >= 0 {
		path = path[:idx]
	}
	// Truncate very long paths
	if len(path) > 60 {
		path = path[:60] + "..."
	}
	return path
}

func classifySeverity(requests int) string {
	switch {
	case requests > 10000:
		return "CRITICAL"
	case requests > 1000:
		return "HIGH"
	case requests > 100:
		return "MEDIUM"
	default:
		return "LOW"
	}
}

func classifyEndpointRisk(endpoint string) string {
	highRisk := []string{"admin", "login", "passwd", "config", "backup", "shell", ".env", "wp-"}
	endpointLower := strings.ToLower(endpoint)
	for _, r := range highRisk {
		if strings.Contains(endpointLower, r) {
			return "HIGH"
		}
	}
	return "LOW"
}

// ---- Aggregation Helpers ----

func sumInts(vals []string) int {
	total := 0
	for _, v := range vals {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			total += n
		}
	}
	return total
}

func uniqueCount(vals []string) int {
	seen := make(map[string]struct{})
	for _, v := range vals {
		seen[v] = struct{}{}
	}
	return len(seen)
}

func uniquePaths(vals []string) []string {
	seen := make(map[string]struct{})
	var result []string
	for _, v := range vals {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}

func toJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
