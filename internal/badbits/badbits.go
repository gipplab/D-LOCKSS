package badbits

import (
	"encoding/csv"
	"log"
	"os"
	"strings"
	"sync"
)

var (
	badBits = struct {
		mu     sync.RWMutex
		cids   map[string]bool
		loaded bool
	}{
		cids: make(map[string]bool),
	}
)

func LoadBadBits(path string) error {
	badBits.mu.Lock()
	defer badBits.mu.Unlock()

	badBits.cids = make(map[string]bool)
	badBits.loaded = false

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[BadBits] File not found: %s (Content blocking disabled)", path)
			return nil
		}
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	if len(records) < 1 {
		log.Printf("[BadBits] Empty file: %s", path)
		return nil
	}

	for i, record := range records {
		// Skip header if it looks like one
		if i == 0 {
			continue
		}
		if len(record) < 1 {
			continue
		}
		cid := strings.TrimSpace(record[0])
		if cid == "" {
			continue
		}

		badBits.cids[cid] = true
	}

	badBits.loaded = true
	log.Printf("[BadBits] Loaded %d blocked CIDs from %s", len(badBits.cids), path)
	return nil
}

func IsCIDBlocked(cid string) bool {
	badBits.mu.RLock()
	defer badBits.mu.RUnlock()

	if !badBits.loaded {
		return false
	}

	cid = strings.TrimSpace(cid)
	return badBits.cids[cid]
}
