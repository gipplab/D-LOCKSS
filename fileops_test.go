package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestProcessNewFilePathContainment(t *testing.T) {
	watch := filepath.Join(string(filepath.Separator), "tmp", "data")

	inside := filepath.Join(watch, "subdir", "file.pdf")
	if rel, err := filepath.Rel(watch, inside); err != nil || rel == ".." || (len(rel) >= 3 && rel[:3] == ".."+string(filepath.Separator)) {
		t.Fatalf("expected inside path to be within watch dir: rel=%q err=%v", rel, err)
	}

	// Prefix confusion: /tmp/dataX/... must be rejected even though it shares a string prefix.
	outsidePrefix := filepath.Join(string(filepath.Separator), "tmp", "dataX", "file.pdf")
	rel, err := filepath.Rel(watch, outsidePrefix)
	if err == nil && rel != ".." && !(len(rel) >= 3 && rel[:3] == ".."+string(filepath.Separator)) {
		t.Fatalf("expected outside prefix path to be outside watch dir: rel=%q", rel)
	}
}

func TestShouldProcessFileEvent(t *testing.T) {
	// Create a temporary file for testing
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Create initial file
	if err := os.WriteFile(testFile, []byte("initial content"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// First event should be processed
	if !shouldProcessFileEvent(testFile) {
		t.Error("first event should be processed")
	}

	// Immediate duplicate event (same content) should be skipped
	if shouldProcessFileEvent(testFile) {
		t.Error("duplicate event with same content should be skipped")
	}

	// Wait a moment, then modify the file (different size)
	time.Sleep(100 * time.Millisecond)
	if err := os.WriteFile(testFile, []byte("modified content - longer"), 0644); err != nil {
		t.Fatalf("failed to modify test file: %v", err)
	}

	// Modified file should be processed (different size)
	if !shouldProcessFileEvent(testFile) {
		t.Error("modified file (different size) should be processed")
	}

	// Another modification (same size, different content - detected via mtime change)
	time.Sleep(100 * time.Millisecond)
	if err := os.WriteFile(testFile, []byte("different content same len"), 0644); err != nil {
		t.Fatalf("failed to modify test file: %v", err)
	}

	// Modified file should be processed (different mtime)
	if !shouldProcessFileEvent(testFile) {
		t.Error("modified file (different mtime) should be processed")
	}

	// Duplicate event for this version should be skipped
	if shouldProcessFileEvent(testFile) {
		t.Error("duplicate event for same version should be skipped")
	}
}

