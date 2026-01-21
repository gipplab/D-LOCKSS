package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBadBitsNormalization(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "badBits.csv")
	data := "CID,Country\nbafyTESTCID, us \n"
	if err := os.WriteFile(p, []byte(data), 0644); err != nil {
		t.Fatalf("write temp badbits: %v", err)
	}

	if err := loadBadBits(p); err != nil {
		t.Fatalf("loadBadBits: %v", err)
	}

	if !isCIDBlocked("bafyTESTCID", "US") {
		t.Fatalf("expected CID to be blocked for US")
	}
	if !isCIDBlocked(" bafyTESTCID ", " us ") {
		t.Fatalf("expected normalization to block CID/country despite whitespace/case")
	}
	if isCIDBlocked("bafyOTHER", "US") {
		t.Fatalf("did not expect other CID to be blocked")
	}
}

