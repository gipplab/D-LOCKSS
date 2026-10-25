package badbits

import (
	"os"
	"path/filepath"
	"testing"
)

func write(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "badBits.csv")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadWithHeader(t *testing.T) {
	f, err := Load(write(t, "cid,reason\nbafyexample1,dmca\nQmExample2,court\n"))
	if err != nil {
		t.Fatal(err)
	}
	if f.Len() != 2 || !f.IsBlocked("bafyexample1") || !f.IsBlocked("QmExample2") {
		t.Fatalf("unexpected filter state: %d entries", f.Len())
	}
	if f.IsBlocked("cid") {
		t.Fatal("header row must be skipped")
	}
}

func TestLoadHeaderlessCIDFirstRow(t *testing.T) {
	f, err := Load(write(t, "bafyfirst\nbafysecond\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !f.IsBlocked("bafyfirst") {
		t.Fatal("a first row that looks like a CID must count")
	}
}

func TestMissingFileIsOpen(t *testing.T) {
	f, err := Load(filepath.Join(t.TempDir(), "nope.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if f.IsBlocked("anything") || f.Len() != 0 {
		t.Fatal("missing file must give an open filter")
	}
}

func TestNilFilterIsOpen(t *testing.T) {
	var f *Filter
	if f.IsBlocked("x") || f.Len() != 0 {
		t.Fatal("nil filter must block nothing")
	}
}

func TestExactMatchOnly(t *testing.T) {
	f, _ := Load(write(t, "bafyblocked\n"))
	if f.IsBlocked("bafyblocked2") || f.IsBlocked("Bafyblocked") {
		t.Fatal("matching must be exact")
	}
}
