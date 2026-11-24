package storage

import (
	"testing"

	"dlockss/internal/config"
)

func TestUnpinForgetsComplete(t *testing.T) {
	sm := NewStorageManager(config.DefaultConfig(), nil, nil)
	sm.PinFile("m")
	sm.MarkComplete("m")
	if !sm.IsComplete("m") {
		t.Fatal("MarkComplete must mark the file complete")
	}
	sm.UnpinFile("m")
	sm.PinFile("m")
	if sm.IsComplete("m") {
		t.Fatal("a file pinned again after an unpin may be incomplete and must be re-pinned")
	}
}
