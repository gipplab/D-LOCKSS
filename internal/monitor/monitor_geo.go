package monitor

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/oschwald/geoip2-golang"
)

const geoCacheTTL = 24 * time.Hour

type geoCacheEntry struct {
	region string
	seen   time.Time
}

// geoResolver resolves IP addresses to human-readable region strings,
// using an optional local MaxMind database and an HTTP fallback.
type geoResolver struct {
	db    *geoip2.Reader
	cache sync.Map
}

func newGeoResolver(dbPath string) *geoResolver {
	g := &geoResolver{}
	if dbPath != "" {
		db, err := geoip2.Open(dbPath)
		if err != nil {
			slog.Error("failed to open geoip database", "path", dbPath, "error", err)
		} else {
			slog.Info("geoip database loaded", "path", dbPath)
			g.db = db
		}
	}
	return g
}

func (g *geoResolver) close() {
	if g.db != nil {
		g.db.Close()
	}
}

func (g *geoResolver) hasDB() bool { return g.db != nil }

func (g *geoResolver) lookupLocalDB(ipStr string) string {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return ""
	}
	record, err := g.db.City(ip)
	if err != nil {
		return ""
	}
	var subdiv string
	if len(record.Subdivisions) > 0 {
		subdiv = record.Subdivisions[0].Names["en"]
	}
	return formatGeoResult(record.Country.IsoCode, record.Country.Names["en"], subdiv)
}

// Resolve returns a region string for the given IP. Uses the local DB
// when available, otherwise an HTTP geo-IP service with caching.
func (g *geoResolver) Resolve(ipStr string) string {
	if ipStr == "" || isPrivateIP(ipStr) {
		return ""
	}
	if g.db != nil {
		return g.lookupLocalDB(ipStr)
	}
	if entry, ok := g.cache.Load(ipStr); ok {
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
		g.cache.Store(ipStr, geoCacheEntry{region: region, seen: time.Now()})
	}
	return region
}

// ResolveFromAddrs extracts IPs from multiaddrs and resolves the region.
func (g *geoResolver) ResolveFromAddrs(addrs []ma.Multiaddr) string {
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
	return g.Resolve(ip)
}

func (g *geoResolver) evictStaleCache() {
	cutoff := time.Now().Add(-geoCacheTTL)
	g.cache.Range(func(key, value interface{}) bool {
		if entry, ok := value.(geoCacheEntry); ok && entry.seen.Before(cutoff) {
			g.cache.Delete(key)
		}
		return true
	})
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
