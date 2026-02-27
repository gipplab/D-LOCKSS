package main

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/oschwald/geoip2-golang"
)

const (
	geoCacheTTL = 24 * time.Hour
)

type geoCacheEntry struct {
	region string
	seen   time.Time
}

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

// preferPublicIP returns the first non-private IP from the list, or the first IP if all are private.
// Use this when a peer has multiple addresses (e.g. LAN + public) so region/geo stays stable.
func preferPublicIP(ips []string) string {
	var fallback string
	for _, ip := range ips {
		if ip == "" {
			continue
		}
		if fallback == "" {
			fallback = ip
		}
		if !isPrivateIP(ip) {
			return ip
		}
	}
	return fallback
}

func (m *Monitor) evictStaleGeoCache() {
	cutoff := time.Now().Add(-geoCacheTTL)
	m.geoCache.Range(func(key, value interface{}) bool {
		if entry, ok := value.(geoCacheEntry); ok && entry.seen.Before(cutoff) {
			m.geoCache.Delete(key)
		}
		return true
	})
}

// resolveGeoIPSync resolves an IP to a region string synchronously.
// Uses local DB if available, otherwise cache, otherwise a direct HTTP call.
func (m *Monitor) resolveGeoIPSync(ipStr string) string {
	if ipStr == "" || isPrivateIP(ipStr) {
		return ""
	}
	if m.geoDB != nil {
		return m.lookupLocalDB(ipStr)
	}
	if entry, ok := m.geoCache.Load(ipStr); ok {
		return entry.(geoCacheEntry).region
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://ip-api.com/json/" + ipStr + "?fields=status,countryCode,regionName,query")
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	var result struct {
		Status      string `json:"status"`
		CountryCode string `json:"countryCode"`
		RegionName  string `json:"regionName"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil || result.Status != "success" {
		return ""
	}
	region := formatGeoResult(result.CountryCode, "", result.RegionName)
	if region != "" {
		m.geoCache.Store(ipStr, geoCacheEntry{region: region, seen: time.Now()})
	}
	return region
}

// resolveRegionFromAddrs extracts IPs from multiaddrs and resolves the region synchronously.
func (m *Monitor) resolveRegionFromAddrs(addrs []ma.Multiaddr) string {
	var ips []string
	for _, addr := range addrs {
		if ipVal, err := addr.ValueForProtocol(ma.P_IP4); err == nil {
			ips = append(ips, ipVal)
		}
		if ipVal, err := addr.ValueForProtocol(ma.P_IP6); err == nil {
			ips = append(ips, ipVal)
		}
	}
	ip := preferPublicIP(ips)
	return m.resolveGeoIPSync(ip)
}

// compile-time check that sync.Map is used (avoids unused import if refactored)
var _ sync.Map
