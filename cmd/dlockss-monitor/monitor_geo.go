package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

func (m *Monitor) geoWorker() {
	client := &http.Client{Timeout: 5 * time.Second}
	ticker := time.NewTicker(1500 * time.Millisecond)
	defer ticker.Stop()

	for req := range m.geoQueue {
		<-ticker.C

		m.mu.RLock()
		if !m.geoCooldownUntil.IsZero() && time.Now().Before(m.geoCooldownUntil) {
			m.mu.RUnlock()
			continue
		}
		cached, found := m.geoCache[req.ip]
		valid := found && time.Since(m.geoCacheTime[req.ip]) < GeoIPCacheDuration
		m.mu.RUnlock()

		if valid {
			m.updateNodeRegion(req.peerID, cached)
			continue
		}

		geo, err := m.fetchGeoLocation(client, req.ip)

		m.mu.Lock()
		if err != nil {
			m.geoFailures++
			if m.geoFailures >= GeoFailureThreshold {
				log.Printf("[Monitor] GeoIP Circuit Breaker TRIPPED. Cooldown for %v.", GeoCooldownDuration)
				m.geoCooldownUntil = time.Now().Add(GeoCooldownDuration)
			}
		} else {
			m.geoFailures = 0
			if geo != nil {
				m.geoCache[req.ip] = geo
				m.geoCacheTime[req.ip] = time.Now()
			}
		}
		m.mu.Unlock()

		if geo != nil {
			m.updateNodeRegion(req.peerID, geo)
		}
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

func (m *Monitor) fetchGeoLocation(client *http.Client, ip string) (*GeoLocation, error) {
	if ip == "" {
		return nil, nil
	}
	if isPrivateIP(ip) {
		return &GeoLocation{Country: "Local Network", RegionName: "LAN", City: "Localhost", CountryCode: "LOC"}, nil
	}
	url := fmt.Sprintf("http://ip-api.com/json/%s?fields=status,country,countryCode,regionName,city", ip)
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 429 {
		return nil, errors.New("rate limit hit")
	}
	var geo GeoLocation
	if err := json.NewDecoder(resp.Body).Decode(&geo); err != nil {
		return nil, err
	}
	if geo.Country == "" {
		return nil, nil
	}
	return &geo, nil
}

func (m *Monitor) updateNodeRegion(peerID string, geo *GeoLocation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if node, exists := m.nodes[peerID]; exists {
		parts := []string{}
		if geo.CountryCode != "" {
			parts = append(parts, geo.CountryCode)
		}
		if geo.RegionName != "" {
			parts = append(parts, geo.RegionName)
		}
		if len(parts) == 0 {
			node.Region = geo.Country
		} else {
			node.Region = strings.Join(parts, " - ")
		}
	}
}
