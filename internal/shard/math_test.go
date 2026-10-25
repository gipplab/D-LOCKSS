package shard

import (
	"crypto/sha256"
	"strings"
	"testing"
)

func TestBitPrefix(t *testing.T) {
	sum := sha256.Sum256([]byte("hello"))
	want := ""
	for i := 0; i < 16; i++ {
		if sum[i/8]&(0x80>>(i%8)) != 0 {
			want += "1"
		} else {
			want += "0"
		}
	}
	if got := BitPrefix("hello", 16); got != want {
		t.Fatalf("BitPrefix = %q, want %q", got, want)
	}
	if got := BitPrefix("hello", 0); got != "" {
		t.Fatalf("depth 0 = %q, want empty", got)
	}
	if got := BitPrefix("hello", 300); len(got) != 256 {
		t.Fatalf("depth 300 gives %d bits, want 256", len(got))
	}
	// Prefix property: deeper prefixes extend shallower ones.
	if !strings.HasPrefix(BitPrefix("x", 20), BitPrefix("x", 7)) {
		t.Fatal("prefix property violated")
	}
}

func TestTargetShardForPayloadClampsDepth(t *testing.T) {
	if got := TargetShardForPayload("cid", 0); len(got) != 1 {
		t.Fatalf("depth 0 should clamp to 1, got %q", got)
	}
	if got := TargetShardForPayload("cid", -5); len(got) != 1 {
		t.Fatalf("negative depth should clamp to 1, got %q", got)
	}
	if got := TargetShardForPayload("cid", 4); got != BitPrefix("cid", 4) {
		t.Fatalf("mismatch with BitPrefix")
	}
}

func TestResponsible(t *testing.T) {
	if !Responsible("", "anything") {
		t.Fatal("the root is responsible for everything")
	}
	key := "some-payload-cid"
	own := BitPrefix(key, 3)
	if !Responsible(own, key) {
		t.Fatal("a shard must cover keys hashing into it")
	}
	if Responsible(Sibling(own), key) {
		t.Fatal("the sibling shard must not cover the key")
	}
}

func TestTreeNavigation(t *testing.T) {
	if Parent("") != "" || Sibling("") != "" {
		t.Fatal("root has no parent or sibling")
	}
	if Parent("01") != "0" {
		t.Fatalf("Parent(01) = %q", Parent("01"))
	}
	if Sibling("01") != "00" || Sibling("00") != "01" {
		t.Fatal("sibling flips the last bit")
	}
	c0, c1 := Children("1")
	if c0 != "10" || c1 != "11" {
		t.Fatalf("Children(1) = %q,%q", c0, c1)
	}
	anc := Ancestors("011")
	want := []string{"01", "0", ""}
	if len(anc) != len(want) {
		t.Fatalf("Ancestors(011) = %v", anc)
	}
	for i := range want {
		if anc[i] != want[i] {
			t.Fatalf("Ancestors(011) = %v, want %v", anc, want)
		}
	}
	if len(Ancestors("")) != 0 {
		t.Fatal("root has no ancestors")
	}
}
