package badbits

import (
	"encoding/csv"
	"log/slog"
	"os"
	"strings"
	"sync"
)

// Filter holds a set of blocked CIDs loaded from a CSV file.
type Filter struct {
	mu     sync.RWMutex
	cids   map[string]bool
	loaded bool
}

// NewFilter loads a bad-bits list from path and returns a Filter.
// If the file does not exist, blocking is disabled (no error).
func NewFilter(path string) (*Filter, error) {
	f := &Filter{cids: make(map[string]bool)}

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Info("badbits file not found, content blocking disabled", "path", path)
			return f, nil
		}
		return f, err
	}
	defer file.Close()

	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return f, err
	}
	if len(records) == 0 {
		slog.Info("badbits file empty", "path", path)
		return f, nil
	}

	for i, record := range records {
		if len(record) == 0 {
			continue
		}
		val := strings.TrimSpace(record[0])
		if val == "" {
			continue
		}
		// The first row is a header unless it holds a CID.
		if i == 0 && !looksLikeCID(val) {
			continue
		}
		f.cids[val] = true
	}

	f.loaded = true
	slog.Info("loaded blocked cids", "count", len(f.cids), "path", path)
	return f, nil
}

func looksLikeCID(s string) bool {
	return strings.HasPrefix(s, "bafy") || strings.HasPrefix(s, "Qm") || strings.HasPrefix(s, "bafk")
}

// IsBlocked returns true if the given CID is in the block list.
// Returns false when the filter is nil or was not loaded.
func (f *Filter) IsBlocked(cid string) bool {
	if f == nil {
		return false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if !f.loaded {
		return false
	}
	return f.cids[strings.TrimSpace(cid)]
}
