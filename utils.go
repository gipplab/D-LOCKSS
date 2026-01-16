package main

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
	"strings"
)

func validateHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil
}

func getBinaryPrefix(s string, depth int) string {
	h := sha256.Sum256([]byte(s))
	return bytesToBinaryString(h[:], depth)
}

func getHexBinaryPrefix(hexStr string, depth int) string {
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		log.Printf("[Error] Failed to decode hex string: %v", err)
		return ""
	}
	return bytesToBinaryString(b, depth)
}

func bytesToBinaryString(b []byte, length int) string {
	var sb strings.Builder
	for _, byteVal := range b {
		for i := 7; i >= 0; i-- {
			if length <= 0 {
				return sb.String()
			}
			if (byteVal>>i)&1 == 1 {
				sb.WriteRune('1')
			} else {
				sb.WriteRune('0')
			}
			length--
		}
	}
	return sb.String()
}
