package badbits

import (
	"encoding/csv"
	"log"
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
			log.Printf("[BadBits] File not found: %s (Content blocking disabled)", path)
			return f, nil
		}
		return f, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return f, err
	}

	if len(records) < 1 {
		log.Printf("[BadBits] Empty file: %s", path)
		return f, nil
	}

	for i, record := range records {
		if len(record) < 1 {
			continue
		}
		val := strings.TrimSpace(record[0])
		if val == "" {
			continue
		}
		if i == 0 && !strings.HasPrefix(val, "bafy") && !strings.HasPrefix(val, "Qm") && !strings.HasPrefix(val, "bafk") {
			continue
		}
		f.cids[val] = true
	}

	f.loaded = true
	log.Printf("[BadBits] Loaded %d blocked CIDs from %s", len(f.cids), path)
	return f, nil
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
