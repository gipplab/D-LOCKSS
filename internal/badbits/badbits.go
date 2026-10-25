// Package badbits implements the content denylist (e.g. DMCA takedowns):
// a CSV of CID strings that the node refuses to ingest, pin, or replicate.
package badbits

import (
	"encoding/csv"
	"os"
	"strings"
	"sync"
)

// Filter is a set of blocked CID strings. The zero/nil Filter blocks nothing.
type Filter struct {
	mu      sync.RWMutex
	blocked map[string]struct{}
}

// Load reads the denylist CSV. The first column of each row is a CID string;
// a header row is skipped unless it looks like a CID. A missing file yields
// an empty (open) filter with no error.
func Load(path string) (*Filter, error) {
	f := &Filter{blocked: map[string]struct{}{}}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return f, nil
		}
		return nil, err
	}
	defer file.Close()

	r := csv.NewReader(file)
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	for i, row := range rows {
		if len(row) == 0 {
			continue
		}
		v := strings.TrimSpace(row[0])
		if v == "" {
			continue
		}
		if i == 0 && !looksLikeCID(v) {
			continue // header
		}
		f.blocked[v] = struct{}{}
	}
	return f, nil
}

func looksLikeCID(s string) bool {
	return strings.HasPrefix(s, "bafy") || strings.HasPrefix(s, "bafk") || strings.HasPrefix(s, "Qm")
}

// IsBlocked reports whether the CID string is denylisted. Matching is exact:
// CIDv0 and CIDv1 forms are distinct entries.
func (f *Filter) IsBlocked(cidStr string) bool {
	if f == nil {
		return false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, ok := f.blocked[cidStr]
	return ok
}

// Len returns the number of blocked CIDs.
func (f *Filter) Len() int {
	if f == nil {
		return 0
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.blocked)
}
