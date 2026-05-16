package jobs

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/semmidev/map-reduce/internal/common"
)

// ============================================================
// 📖 USE CASE: Back-of-Book Index Generation
// ============================================================
//
// The input "filename" encodes the PDF page number:
//   filename = "page:42"
//   content  = extracted text of that page
//
// Map phase:
//   → tokenizes words, filters stop words, emits (term, pageNum)
//   → detects explicit phrases (--phrases flag)
//   → emits adjacent-word bigrams for automatic phrase detection
//
// Reduce phase:
//   → receives (term, [pageNum, pageNum, ...])
//   → deduplicates, sorts page numbers
//   → outputs {"pages":[12,15,19],"count":3}
// ============================================================

// IndexConfig carries runtime options into the map/reduce functions.
type IndexConfig struct {
	MinWordLength int
	ExcludeWords  map[string]struct{} // merged with built-in stop words
	TopN          int
	Phrases       []string // explicit multi-word phrases to detect
}

var (
	// Strip anything that isn't alphanumeric, space, hyphen, or apostrophe
	punctRe = regexp.MustCompile(`[^a-zA-Z0-9\s'-]`)

	// Valid word tokens: letters, optionally with inner hyphens/apostrophes
	wordRe = regexp.MustCompile(`[a-zA-Z][a-zA-Z'-]*[a-zA-Z]|[a-zA-Z]`)

	// Purely numeric — skip these
	numericRe = regexp.MustCompile(`^\d+$`)
)

// DefaultStopWords is the built-in English stop-word set.
// Users extend this via --exclude.
var DefaultStopWords = map[string]struct{}{
	"the": {}, "and": {}, "for": {}, "with": {}, "that": {},
	"this": {}, "from": {}, "have": {}, "been": {}, "will": {},
	"are": {}, "was": {}, "but": {}, "not": {}, "you": {},
	"all": {}, "can": {}, "her": {}, "him": {}, "his": {},
	"its": {}, "our": {}, "out": {}, "use": {}, "one": {},
	"who": {}, "she": {}, "they": {}, "then": {}, "than": {},
	"also": {}, "into": {}, "more": {}, "some": {}, "has": {},
	"when": {}, "which": {}, "there": {}, "about": {}, "would": {},
	"your": {}, "what": {}, "each": {}, "how": {}, "may": {},
	"any": {}, "such": {}, "only": {}, "other": {}, "their": {},
	"were": {}, "does": {}, "had": {}, "way": {}, "two": {},
	"very": {}, "just": {}, "over": {}, "like": {}, "make": {},
	"them": {}, "time": {}, "said": {}, "get": {}, "know": {},
	"take": {}, "even": {}, "back": {}, "good": {}, "much": {},
	"well": {}, "need": {}, "same": {}, "see": {}, "new": {},
	"now": {}, "per": {}, "via": {}, "etc": {},
}

// BuildIndexFuncs returns a configured (MapFunc, ReduceFunc) pair as closures,
// injecting runtime options without changing the canonical function signatures.
func BuildIndexFuncs(cfg IndexConfig) (MapFunc, ReduceFunc) {
	// Merge user exclusions into a unified stop-word set
	stopWords := make(map[string]struct{}, len(DefaultStopWords)+len(cfg.ExcludeWords))
	for k, v := range DefaultStopWords {
		stopWords[k] = v
	}
	for k, v := range cfg.ExcludeWords {
		stopWords[k] = v
	}

	// Lowercase phrases for fast substring search
	lowerPhrases := make([]string, 0, len(cfg.Phrases))
	for _, p := range cfg.Phrases {
		if p = strings.ToLower(strings.TrimSpace(p)); p != "" {
			lowerPhrases = append(lowerPhrases, p)
		}
	}

	mapFn := func(filename, content string) []common.KeyValue {
		// Decode page number from synthetic filename "page:<n>"
		pageNum := decodePageNum(filename)
		pageStr := strconv.Itoa(pageNum)

		// Normalize: strip punctuation, lowercase
		cleaned := punctRe.ReplaceAllString(content, " ")
		lower := strings.ToLower(cleaned)
		rawWords := wordRe.FindAllString(lower, -1)

		// Trim leading/trailing hyphens and apostrophes
		words := make([]string, 0, len(rawWords))
		for _, w := range rawWords {
			if w = strings.Trim(w, "-'"); w != "" {
				words = append(words, w)
			}
		}

		var kvs []common.KeyValue

		// ── Single-word terms ──────────────────────────────────────
		for _, w := range words {
			if len(w) < cfg.MinWordLength {
				continue
			}
			if _, stop := stopWords[w]; stop {
				continue
			}
			if numericRe.MatchString(w) {
				continue
			}
			kvs = append(kvs, common.KeyValue{Key: "TERM:" + w, Value: pageStr})
		}

		// ── Explicit phrase detection (--phrases flag) ─────────────
		for _, phrase := range lowerPhrases {
			if strings.Contains(lower, phrase) {
				kvs = append(kvs, common.KeyValue{Key: "PHRASE:" + phrase, Value: pageStr})
			}
		}

		// ── Automatic bigram indexing ──────────────────────────────
		// Adjacent pairs of significant words surface collocations like
		// "distributed systems", "hash table", "binary search", etc.
		for i := 0; i+1 < len(words); i++ {
			w1, w2 := words[i], words[i+1]
			if len(w1) < cfg.MinWordLength || len(w2) < cfg.MinWordLength {
				continue
			}
			if _, stop := stopWords[w1]; stop {
				continue
			}
			if _, stop := stopWords[w2]; stop {
				continue
			}
			if numericRe.MatchString(w1) || numericRe.MatchString(w2) {
				continue
			}
			kvs = append(kvs, common.KeyValue{Key: "BIGRAM:" + w1 + " " + w2, Value: pageStr})
		}

		return kvs
	}

	reduceFn := func(key string, values []string) string {
		// Deduplicate and sort page numbers
		seen := make(map[int]struct{}, len(values))
		for _, v := range values {
			if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
				seen[n] = struct{}{}
			}
		}
		pages := make([]int, 0, len(seen))
		for p := range seen {
			pages = append(pages, p)
		}
		sort.Ints(pages)

		strs := make([]string, len(pages))
		for i, p := range pages {
			strs[i] = strconv.Itoa(p)
		}
		return fmt.Sprintf(`{"pages":[%s],"count":%d}`, strings.Join(strs, ","), len(pages))
	}

	return mapFn, reduceFn
}

// decodePageNum parses "page:42" → 42. Returns 0 on failure.
func decodePageNum(filename string) int {
	if after, ok := strings.CutPrefix(filename, "page:"); ok {
		if n, err := strconv.Atoi(after); err == nil {
			return n
		}
	}
	return 0
}
