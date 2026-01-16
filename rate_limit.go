package main

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func checkRateLimit(peerID peer.ID) bool {
	rateLimiter.Lock()
	defer rateLimiter.Unlock()

	prl, exists := rateLimiter.peers[peerID]
	if !exists {
		prl = &peerRateLimit{
			messages: make([]time.Time, 0),
		}
		rateLimiter.peers[peerID] = prl
	}

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
			rateLimiter.Lock()
			now := time.Now()
			cutoff := now.Add(-RateLimitWindow * 2)

			for peerID, peerLimit := range rateLimiter.peers {
				peerLimit.mu.Lock()
				hasRecent := false
				for _, msgTime := range peerLimit.messages {
					if msgTime.After(cutoff) {
						hasRecent = true
						break
					}
				}
				peerLimit.mu.Unlock()

				if !hasRecent {
					delete(rateLimiter.peers, peerID)
				}
			}
			rateLimiter.Unlock()
		}
	}
}
