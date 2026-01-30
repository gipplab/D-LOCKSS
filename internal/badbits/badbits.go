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
		mu    sync.RWMutex
		cids  map[string]map[string]bool
		loaded bool
	}{
		cids: make(map[string]map[string]bool),
	}
)

func LoadBadBits(path string) error {
	badBits.mu.Lock()
	defer badBits.mu.Unlock()

	badBits.cids = make(map[string]map[string]bool)
	badBits.loaded = false

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[BadBits] File not found: %s (DMCA blocking disabled)", path)
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

	if len(records) < 2 {
		log.Printf("[BadBits] Empty or header-only file: %s", path)
		return nil
	}

	for i, record := range records {
		if i == 0 {
			continue
		}
		if len(record) < 2 {
			continue
		}
		cid := strings.TrimSpace(record[0])
		country := strings.ToUpper(strings.TrimSpace(record[1]))
		if cid == "" || country == "" {
			continue
		}

		if badBits.cids[cid] == nil {
			badBits.cids[cid] = make(map[string]bool)
		}
		badBits.cids[cid][country] = true
	}

	badBits.loaded = true
	log.Printf("[BadBits] Loaded %d blocked CID entries from %s", len(badBits.cids), path)
	return nil
}

func IsCIDBlocked(cid string, country string) bool {
	badBits.mu.RLock()
	defer badBits.mu.RUnlock()

	if !badBits.loaded {
		return false
	}

	cid = strings.TrimSpace(cid)
	country = strings.ToUpper(strings.TrimSpace(country))
	countries, exists := badBits.cids[cid]
	if !exists {
		return false
	}

	return countries[country]
}
