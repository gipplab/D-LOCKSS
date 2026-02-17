package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/oschwald/geoip2-golang"
)

const (
	geoAPIBatchURL      = "http://ip-api.com/batch?fields=status,countryCode,regionName,query"
	geoAPIMaxBatch      = 100
	geoAPIBatchInterval = 5 * time.Second
	geoAPITimeout       = 10 * time.Second
	geoMaxQueueSize     = 500
)

// openGeoIPDB opens a MaxMind-format .mmdb file for local geo lookups.
// Returns nil if path is empty or the file cannot be opened.
func openGeoIPDB(path string) *geoip2.Reader {
	if path == "" {
		return nil
	}
	db, err := geoip2.Open(path)
	if err != nil {
		log.Printf("[Monitor] Failed to open GeoIP database %s: %v", path, err)
		return nil
	}
	log.Printf("[Monitor] GeoIP database loaded: %s", path)
	return db
}

// lookupGeoIP resolves an IP to a region string like "US - California".
// With a local DB: instant. Without: checks cache, enqueues for async API lookup.
// Returns "" only when the result is pending API resolution.
func (m *Monitor) lookupGeoIP(ipStr string) string {
	if ipStr == "" {
		return ""
	}
	if isPrivateIP(ipStr) {
		return "LOC - LAN"
	}
	if m.geoDB != nil {
		return m.lookupLocalDB(ipStr)
	}
	if region, ok := m.geoCache.Load(ipStr); ok {
		return region.(string)
	}
	select {
	case m.geoQueue <- ipStr:
	default:
	}
	return ""
}

func (m *Monitor) lookupLocalDB(ipStr string) string {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return ""
	}
	record, err := m.geoDB.City(ip)
	if err != nil {
		return ""
	}
	var subdiv string
	if len(record.Subdivisions) > 0 {
		subdiv = record.Subdivisions[0].Names["en"]
	}
	return formatGeoResult(record.Country.IsoCode, record.Country.Names["en"], subdiv)
}

func formatGeoResult(countryCode, countryName, region string) string {
	var parts []string
	if countryCode != "" {
		parts = append(parts, countryCode)
	}
	if region != "" {
		parts = append(parts, region)
	}
	if len(parts) == 0 {
		if countryName != "" {
			return countryName
		}
		return ""
	}
	return strings.Join(parts, " - ")
}

// geoAPIWorker processes queued IPs in batches via the ip-api.com batch endpoint.
// Started only when no local .mmdb database is configured.
func (m *Monitor) geoAPIWorker() {
	client := &http.Client{Timeout: geoAPITimeout}
	ticker := time.NewTicker(geoAPIBatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			m.processGeoBatch(client)
		}
	}
}

func (m *Monitor) processGeoBatch(client *http.Client) {
	seen := make(map[string]bool)
	var batch []string
	for len(batch) < geoAPIMaxBatch {
		select {
		case ip := <-m.geoQueue:
			if _, cached := m.geoCache.Load(ip); cached {
				continue
			}
			if seen[ip] {
				continue
			}
			seen[ip] = true
			batch = append(batch, ip)
		default:
			goto drained
		}
	}
drained:
	if len(batch) == 0 {
		return
	}

	type batchEntry struct {
		Query string `json:"query"`
	}
	entries := make([]batchEntry, len(batch))
	for i, ip := range batch {
		entries[i] = batchEntry{Query: ip}
	}
	body, err := json.Marshal(entries)
	if err != nil {
		return
	}

	resp, err := client.Post(geoAPIBatchURL, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("[GeoIP] API request failed: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		log.Printf("[GeoIP] Rate limited (429), will retry next cycle")
		return
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("[GeoIP] API returned status %d", resp.StatusCode)
		return
	}

	type batchResult struct {
		Status      string `json:"status"`
		Query       string `json:"query"`
		CountryCode string `json:"countryCode"`
		RegionName  string `json:"regionName"`
	}
	var results []batchResult
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		log.Printf("[GeoIP] Failed to decode response: %v", err)
		return
	}

	resolved := make(map[string]string)
	for _, r := range results {
		if r.Status != "success" {
			continue
		}
		region := formatGeoResult(r.CountryCode, "", r.RegionName)
		if region != "" {
			m.geoCache.Store(r.Query, region)
			resolved[r.Query] = region
		}
	}

	if len(resolved) > 0 {
		m.mu.Lock()
		for _, node := range m.nodes {
			if node.IPAddress != "" && node.Region == "" {
				if region, ok := resolved[node.IPAddress]; ok {
					node.Region = region
				}
			}
		}
		m.mu.Unlock()
		log.Printf("[GeoIP] Resolved %d/%d IPs via API", len(resolved), len(batch))
	}
}

func isPrivateIP(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	privateIPBlocks := []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
	for _, cidr := range privateIPBlocks {
		_, block, _ := net.ParseCIDR(cidr)
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

// compile-time check that sync.Map is used (avoids unused import if refactored)
var _ sync.Map
