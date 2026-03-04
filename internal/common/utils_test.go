package common

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func getHexPrefixFor(s string, depth int) string {
	h := sha256.Sum256([]byte(s))
	result, _ := GetHexBinaryPrefix(hex.EncodeToString(h[:]), depth)
	return result
}

func mustGetHexBinaryPrefix(hexStr string, depth int) string {
	result, _ := GetHexBinaryPrefix(hexStr, depth)
	return result
}

func TestValidateHash(t *testing.T) {
	tests := []struct {
		name  string
		hash  string
		valid bool
	}{
		{
			name:  "valid 64-char hex",
			hash:  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			valid: true,
		},
		{
			name:  "valid all zeros",
			hash:  "0000000000000000000000000000000000000000000000000000000000000000",
			valid: true,
		},
		{
			name:  "valid all f",
			hash:  "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			valid: true,
		},
		{
			name:  "too short",
			hash:  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85",
			valid: false,
		},
		{
			name:  "too long",
			hash:  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8550",
			valid: false,
		},
		{
			name:  "non-hex chars",
			hash:  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85g",
			valid: false,
		},
		{
			name:  "uppercase non-hex",
			hash:  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85G",
			valid: false,
		},
		{
			name:  "empty",
			hash:  "",
			valid: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ValidateHash(tt.hash)
			if got != tt.valid {
				t.Errorf("ValidateHash(%q) = %v, want %v", tt.hash, got, tt.valid)
			}
		})
	}
}

func TestGetBinaryPrefix(t *testing.T) {
	// Known SHA256 outputs for verification at various depths.
	// SHA256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
	// First byte 0xe3 = 11100011 in binary (MSB first)
	emptyHash := sha256.Sum256([]byte(""))
	emptyHex := hex.EncodeToString(emptyHash[:])

	// SHA256("hello") = 2cf24dba5fb0a30e26e83b2c5e9e4e1b6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e - verify
	helloHash := sha256.Sum256([]byte("hello"))
	helloHex := hex.EncodeToString(helloHash[:])

	tests := []struct {
		name   string
		input  string
		depth  int
		expect string // computed via GetHexBinaryPrefix(hex(sha256(input)), depth)
	}{
		{"empty string depth 0", "", 0, ""},
		{"empty string depth 1", "", 1, mustGetHexBinaryPrefix(emptyHex, 1)},
		{"empty string depth 8", "", 8, mustGetHexBinaryPrefix(emptyHex, 8)},
		{"empty string depth 16", "", 16, mustGetHexBinaryPrefix(emptyHex, 16)},
		{"hello depth 0", "hello", 0, ""},
		{"hello depth 1", "hello", 1, mustGetHexBinaryPrefix(helloHex, 1)},
		{"hello depth 8", "hello", 8, mustGetHexBinaryPrefix(helloHex, 8)},
		{"hello depth 32", "hello", 32, mustGetHexBinaryPrefix(helloHex, 32)},
		{"a depth 8", "a", 8, getHexPrefixFor("a", 8)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetBinaryPrefix(tt.input, tt.depth)
			if got != tt.expect {
				t.Errorf("GetBinaryPrefix(%q, %d) = %q, want %q", tt.input, tt.depth, got, tt.expect)
			}
		})
	}

	// Verify known SHA256("") first 8 bits: 0xe3 = 11100011
	got := GetBinaryPrefix("", 8)
	want := "11100011"
	if got != want {
		t.Errorf("GetBinaryPrefix(\"\", 8) = %q, want %q (SHA256 of empty)", got, want)
	}
}

func TestGetHexBinaryPrefix(t *testing.T) {
	// Valid hex: SHA256 of "" = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
	validHex := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	tests := []struct {
		name    string
		hexStr  string
		depth   int
		want    string
		wantErr bool
	}{
		{"valid hex depth 1", validHex, 1, "1", false},
		{"valid hex depth 8", validHex, 8, "11100011", false},
		{"valid hex depth 16", validHex, 16, "1110001110110000", false},
		{"valid hex depth 0", validHex, 0, "", false},
		{"invalid hex odd length", "e3b0c", 8, "", true},
		{"invalid hex non-hex char", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85g", 8, "", true},
		{"invalid hex single non-hex", "g", 1, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetHexBinaryPrefix(tt.hexStr, tt.depth)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetHexBinaryPrefix(%q, %d) error = %v, wantErr %v", tt.hexStr, tt.depth, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("GetHexBinaryPrefix(%q, %d) = %q, want %q", tt.hexStr, tt.depth, got, tt.want)
			}
		})
	}
}

func TestKeyToStableHex(t *testing.T) {
	// Deterministic: same input must produce same output
	key := "QmSomeCIDv1"
	got1 := KeyToStableHex(key)
	got2 := KeyToStableHex(key)
	if got1 != got2 {
		t.Errorf("KeyToStableHex(%q) not deterministic: %q vs %q", key, got1, got2)
	}
	// Output must be 64-char hex (SHA256)
	if len(got1) != 64 {
		t.Errorf("KeyToStableHex(%q) len = %d, want 64", key, len(got1))
	}
	if !ValidateHash(got1) {
		t.Errorf("KeyToStableHex(%q) = %q is not valid hex", key, got1)
	}
	// Different keys produce different output
	other := KeyToStableHex("QmOtherCIDv1")
	if got1 == other {
		t.Errorf("KeyToStableHex different keys produced same output")
	}
}

func TestTargetShardForPayload(t *testing.T) {
	payload := "QmPayloadCID123"

	// Consistency: same input produces same output
	got1, err := TargetShardForPayload(payload, 8)
	if err != nil {
		t.Fatalf("TargetShardForPayload error: %v", err)
	}
	got2, _ := TargetShardForPayload(payload, 8)
	if got1 != got2 {
		t.Errorf("TargetShardForPayload not consistent: %q vs %q", got1, got2)
	}

	// depth < 1 defaults to 1
	got0, _ := TargetShardForPayload(payload, 0)
	got1depth, _ := TargetShardForPayload(payload, 1)
	if got0 != got1depth {
		t.Errorf("TargetShardForPayload(payload, 0) = %q, want same as depth 1: %q", got0, got1depth)
	}

	gotNeg, _ := TargetShardForPayload(payload, -5)
	if gotNeg != got1depth {
		t.Errorf("TargetShardForPayload(payload, -5) = %q, want same as depth 1: %q", gotNeg, got1depth)
	}

	// depth 1 returns single bit
	if len(got1depth) != 1 {
		t.Errorf("TargetShardForPayload(_, 1) len = %d, want 1", len(got1depth))
	}
	if got1depth != "0" && got1depth != "1" {
		t.Errorf("TargetShardForPayload(_, 1) = %q, want \"0\" or \"1\"", got1depth)
	}

	// depth 8 returns 8 bits
	got8, _ := TargetShardForPayload(payload, 8)
	if len(got8) != 8 {
		t.Errorf("TargetShardForPayload(_, 8) len = %d, want 8", len(got8))
	}
}
