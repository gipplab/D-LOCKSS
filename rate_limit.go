package main

import (
	"context"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func checkRateLimit(peerID peer.ID) bool {
	return checkRateLimitForMessage(peerID, "")
}

func checkRateLimitForMessage(peerID peer.ID, messageType string) bool {
	if messageType == "DELEGATE" && !canAcceptCustodialFile() {
		return false
	}

	prl := rateLimiter.GetOrCreate(peerID)

	prl.mu.Lock()
	defer prl.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-RateLimitWindow)

	validMessages := make([]time.Time, 0, len(prl.messages))
	for _, msgTime := range prl.messages {
		if msgTime.After(cutoff) {
			validMessages = append(validMessages, msgTime)
		}
	}

	if len(validMessages) >= MaxMessagesPerWindow {
		return false
	}

	validMessages = append(validMessages, now)
	prl.messages = validMessages
	return true
}

func runRateLimiterCleanup(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			cutoff := now.Add(-RateLimitWindow * 2)
			removed := rateLimiter.Cleanup(cutoff)
			if removed > 0 {
				log.Printf("[RateLimit] Cleaned up %d inactive peer rate limiters", removed)
			}
		}
	}
}
