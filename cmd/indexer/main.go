package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/semmidev/map-reduce/internal/common"
	"github.com/semmidev/map-reduce/jobs"
)

// ============================================================
// 📖 Book Index Generator — CLI Entry Point
// ============================================================
//
// Usage:
//   indexer --input=ebook.pdf --workers=4 --min-word-length=4 \
//           --exclude=the,and,for --top-n=500 --output=index.json
//           --phrases="distributed systems,hash table"
//
// Supported output formats (determined by extension):
//   .json  → structured JSON array
//   .txt   → plain text columnar index
//   .md    → markdown with alphabetical sections
// ============================================================

func main() {
	// ── CLI Flags ──────────────────────────────────────────────────────────
	inputFlag := flag.String("input", "", "Path to input PDF file (required)")
	workersFlag := flag.Int("workers", 4, "Number of parallel worker goroutines")
	minLenFlag := flag.Int("min-word-length", 4, "Minimum word length to index")
	excludeFlag := flag.String("exclude", "", "Comma-separated words to exclude (on top of built-in stop words)")
	topNFlag := flag.Int("top-n", 500, "Maximum number of index terms (0 = no limit)")
	outputFlag := flag.String("output", "index.json", "Output file path (.json, .txt, or .md)")
	phrasesFlag := flag.String("phrases", "", "Comma-separated phrases to detect (e.g. 'distributed systems,hash table')")
	nReduceFlag := flag.Int("n-reduce", 4, "Number of reduce partitions (tune for large PDFs)")
	flag.Parse()

	if *inputFlag == "" {
		fmt.Fprintln(os.Stderr, "Error: --input is required")
		flag.Usage()
		os.Exit(1)
	}

	// ── Build config ───────────────────────────────────────────────────────
	excludeWords := make(map[string]struct{})
	if *excludeFlag != "" {
		for _, w := range strings.Split(*excludeFlag, ",") {
			w = strings.TrimSpace(strings.ToLower(w))
			if w != "" {
				excludeWords[w] = struct{}{}
			}
		}
	}

	var phrases []string
	if *phrasesFlag != "" {
		for _, p := range strings.Split(*phrasesFlag, ",") {
			p = strings.TrimSpace(strings.ToLower(p))
			if p != "" {
				phrases = append(phrases, p)
			}
		}
	}

	cfg := jobs.IndexConfig{
		MinWordLength: *minLenFlag,
		ExcludeWords:  excludeWords,
		TopN:          *topNFlag,
		Phrases:       phrases,
	}
	mapFn, reduceFn := jobs.BuildIndexFuncs(cfg)

	// ── Extract PDF pages ──────────────────────────────────────────────────
	log.Printf("[INDEXER] Extracting text from: %s", *inputFlag)
	pages, err := extractPDFPages(*inputFlag)
	if err != nil {
		log.Fatalf("[INDEXER] PDF extraction failed: %v\n  → Make sure poppler-utils is installed (apt/apk: poppler-utils)", err)
	}
	if len(pages) == 0 {
		log.Fatalf("[INDEXER] No pages extracted — is the PDF text-based (not scanned)?")
	}
	log.Printf("[INDEXER] Extracted %d pages", len(pages))

	// ── Run in-process MapReduce ───────────────────────────────────────────
	start := time.Now()
	index, err := runMapReduce(pages, mapFn, reduceFn, *workersFlag, *nReduceFlag)
	if err != nil {
		log.Fatalf("[INDEXER] MapReduce failed: %v", err)
	}
	log.Printf("[INDEXER] MapReduce completed in %.2fs — %d unique terms before filtering",
		time.Since(start).Seconds(), len(index))

	// ── Apply TopN filter (by occurrence count) ────────────────────────────
	if *topNFlag > 0 && len(index) > *topNFlag {
		type entry struct {
			term  string
			count int
		}
		entries := make([]entry, 0, len(index))
		for term, pages := range index {
			entries = append(entries, entry{term, len(pages)})
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].count > entries[j].count
		})
		filtered := make(map[string][]int)
		for _, e := range entries[:*topNFlag] {
			filtered[e.term] = index[e.term]
		}
		index = filtered
		log.Printf("[INDEXER] Filtered to top %d terms by page frequency", *topNFlag)
	}

	// ── Write output ───────────────────────────────────────────────────────
	ext := strings.ToLower(filepath.Ext(*outputFlag))
	switch ext {
	case ".json":
		err = writeJSON(index, *outputFlag)
	case ".md":
		err = writeMarkdown(index, *outputFlag)
	default: // .txt or anything else
		err = writePlainText(index, *outputFlag)
	}
	if err != nil {
		log.Fatalf("[INDEXER] Failed to write output: %v", err)
	}
	log.Printf("[INDEXER] ✅ Index written → %s (%d terms)", *outputFlag, len(index))
}

// ─────────────────────────────────────────────────────────────────────────────
// PDF Extraction
// ─────────────────────────────────────────────────────────────────────────────

type pdfPage struct {
	Number  int
	Content string
}

// extractPDFPages extracts page text using pdftotext (poppler-utils).
// Each page is extracted individually to preserve page number mapping.
func extractPDFPages(pdfPath string) ([]pdfPage, error) {
	if _, err := exec.LookPath("pdftotext"); err != nil {
		return nil, fmt.Errorf("pdftotext not found — install poppler-utils")
	}

	totalPages, err := getPDFPageCount(pdfPath)
	if err != nil {
		return nil, fmt.Errorf("count pages: %w", err)
	}
	log.Printf("[INDEXER] PDF has %d pages", totalPages)

	var (
		mu    sync.Mutex
		pages []pdfPage
		wg    sync.WaitGroup
		sem   = make(chan struct{}, 8) // max 8 concurrent pdftotext calls
	)

	for i := 1; i <= totalPages; i++ {
		wg.Add(1)
		go func(pageNum int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			text, err := runPDFToText(pdfPath, pageNum)
			if err != nil || strings.TrimSpace(text) == "" {
				return
			}
			mu.Lock()
			pages = append(pages, pdfPage{Number: pageNum, Content: text})
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	sort.Slice(pages, func(i, j int) bool {
		return pages[i].Number < pages[j].Number
	})
	return pages, nil
}

func getPDFPageCount(path string) (int, error) {
	// Try pdfinfo (faster)
	if out, err := exec.Command("pdfinfo", path).Output(); err == nil {
		for _, line := range strings.Split(string(out), "\n") {
			if strings.HasPrefix(line, "Pages:") {
				parts := strings.Fields(line)
				if len(parts) >= 2 {
					if n, err := strconv.Atoi(parts[1]); err == nil {
						return n, nil
					}
				}
			}
		}
	}
	// Fallback: extract all text, count form-feed characters (\x0c = page separator)
	out, err := exec.Command("pdftotext", path, "-").Output()
	if err != nil {
		return 0, fmt.Errorf("pdftotext failed: %w", err)
	}
	n := strings.Count(string(out), "\x0c")
	if n == 0 {
		n = 1
	}
	return n, nil
}

func runPDFToText(path string, page int) (string, error) {
	ps := strconv.Itoa(page)
	out, err := exec.Command("pdftotext", "-f", ps, "-l", ps, path, "-").Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// ─────────────────────────────────────────────────────────────────────────────
// In-process MapReduce Engine
// ─────────────────────────────────────────────────────────────────────────────

func runMapReduce(
	pages []pdfPage,
	mapFn jobs.MapFunc,
	reduceFn jobs.ReduceFunc,
	nWorkers, nReduce int,
) (map[string][]int, error) {

	// ── MAP PHASE ────────────────────────────────────────────────────────────
	type mapJob struct {
		filename string
		content  string
	}

	jobCh := make(chan mapJob, len(pages))
	for _, p := range pages {
		jobCh <- mapJob{
			filename: fmt.Sprintf("page:%d", p.Number),
			content:  p.Content,
		}
	}
	close(jobCh)

	// Partition KV pairs into reduce buckets
	buckets := make([][]common.KeyValue, nReduce)
	for i := range buckets {
		buckets[i] = make([]common.KeyValue, 0)
	}
	var bucketMu sync.Mutex

	var mapWG sync.WaitGroup
	for i := 0; i < nWorkers; i++ {
		mapWG.Add(1)
		go func() {
			defer mapWG.Done()
			// Thread-local accumulation to reduce lock contention
			local := make([][]common.KeyValue, nReduce)
			for j := range local {
				local[j] = make([]common.KeyValue, 0)
			}
			for job := range jobCh {
				kvs := mapFn(job.filename, job.content)
				for _, kv := range kvs {
					b := ihash(kv.Key) % nReduce
					local[b] = append(local[b], kv)
				}
			}
			bucketMu.Lock()
			for b := range local {
				buckets[b] = append(buckets[b], local[b]...)
			}
			bucketMu.Unlock()
		}()
	}
	mapWG.Wait()

	log.Printf("[INDEXER] Map phase done. Proceeding to reduce...")

	// ── REDUCE PHASE ─────────────────────────────────────────────────────────
	var (
		indexMu    sync.Mutex
		finalIndex = make(map[string][]int)
	)

	bucketCh := make(chan int, nReduce)
	for i := range buckets {
		bucketCh <- i
	}
	close(bucketCh)

	var reduceWG sync.WaitGroup
	for i := 0; i < nWorkers; i++ {
		reduceWG.Add(1)
		go func() {
			defer reduceWG.Done()
			for b := range bucketCh {
				kvs := buckets[b]
				sortKVs(kvs)

				j := 0
				for j < len(kvs) {
					key := kvs[j].Key
					k := j
					for k < len(kvs) && kvs[k].Key == key {
						k++
					}
					vals := make([]string, 0, k-j)
					for x := j; x < k; x++ {
						vals = append(vals, kvs[x].Value)
					}
					// Parse page numbers directly (avoids round-tripping JSON)
					pageNums := uniqueInts(vals)
					if len(pageNums) > 0 {
						displayKey := formatKey(key)
						indexMu.Lock()
						finalIndex[displayKey] = mergeInts(finalIndex[displayKey], pageNums)
						indexMu.Unlock()
					}
					j = k
				}
			}
		}()
	}
	reduceWG.Wait()

	return finalIndex, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Output Writers
// ─────────────────────────────────────────────────────────────────────────────

// IndexEntry is the serialized form of one index term.
type IndexEntry struct {
	Term  string `json:"term"`
	Pages []int  `json:"pages"`
	Count int    `json:"count"`
}

func sortedEntries(index map[string][]int) []IndexEntry {
	entries := make([]IndexEntry, 0, len(index))
	for term, pages := range index {
		sorted := make([]int, len(pages))
		copy(sorted, pages)
		sort.Ints(sorted)
		entries = append(entries, IndexEntry{Term: term, Pages: sorted, Count: len(sorted)})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Term < entries[j].Term
	})
	return entries
}

func writeJSON(index map[string][]int, path string) error {
	entries := sortedEntries(index)
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func writePlainText(index map[string][]int, path string) error {
	entries := sortedEntries(index)
	var sb strings.Builder
	sb.WriteString("BOOK INDEX\n")
	sb.WriteString(strings.Repeat("=", 72) + "\n\n")
	for _, e := range entries {
		pages := make([]string, len(e.Pages))
		for i, p := range e.Pages {
			pages[i] = strconv.Itoa(p)
		}
		line := fmt.Sprintf("%-40s %s\n", e.Term, strings.Join(pages, ", "))
		sb.WriteString(line)
	}
	return os.WriteFile(path, []byte(sb.String()), 0644)
}

func writeMarkdown(index map[string][]int, path string) error {
	entries := sortedEntries(index)
	var sb strings.Builder
	sb.WriteString("# Book Index\n\n")
	sb.WriteString(fmt.Sprintf("*%d indexed terms*\n", len(entries)))

	currentLetter := ""
	for _, e := range entries {
		if len(e.Term) == 0 {
			continue
		}
		first := strings.ToUpper(string([]rune(e.Term)[0]))
		if first != currentLetter {
			currentLetter = first
			sb.WriteString(fmt.Sprintf("\n## %s\n\n", currentLetter))
		}
		pages := make([]string, len(e.Pages))
		for i, p := range e.Pages {
			pages[i] = strconv.Itoa(p)
		}
		sb.WriteString(fmt.Sprintf("- **%s** — %s\n", e.Term, strings.Join(pages, ", ")))
	}
	return os.WriteFile(path, []byte(sb.String()), 0644)
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// formatKey strips the internal prefix (TERM:, PHRASE:, BIGRAM:) for display.
func formatKey(key string) string {
	for _, prefix := range []string{"TERM:", "PHRASE:", "BIGRAM:"} {
		if after, ok := strings.CutPrefix(key, prefix); ok {
			return after
		}
	}
	return key
}

func uniqueInts(vals []string) []int {
	seen := make(map[int]struct{})
	for _, v := range vals {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			seen[n] = struct{}{}
		}
	}
	result := make([]int, 0, len(seen))
	for n := range seen {
		result = append(result, n)
	}
	sort.Ints(result)
	return result
}

func mergeInts(a, b []int) []int {
	seen := make(map[int]struct{})
	for _, n := range a {
		seen[n] = struct{}{}
	}
	for _, n := range b {
		seen[n] = struct{}{}
	}
	result := make([]int, 0, len(seen))
	for n := range seen {
		result = append(result, n)
	}
	sort.Ints(result)
	return result
}

func sortKVs(kvs []common.KeyValue) {
	sort.Slice(kvs, func(i, j int) bool {
		if kvs[i].Key != kvs[j].Key {
			return kvs[i].Key < kvs[j].Key
		}
		return kvs[i].Value < kvs[j].Value
	})
}

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
