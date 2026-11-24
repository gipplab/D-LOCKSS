package ipfs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuthHeader(t *testing.T) {
	for secret, want := range map[string]string{
		"s3cret":             "Bearer s3cret",
		"bearer:s3cret":      "Bearer s3cret",
		"basic:alice:pw":     "Basic YWxpY2U6cHc=",
		"basic:YWxpY2U6cHc=": "Basic YWxpY2U6cHc=",
	} {
		got, err := authHeader(secret)
		if err != nil || got != want {
			t.Errorf("authHeader(%q) = %q, %v; want %q", secret, got, err, want)
		}
	}
	if _, err := authHeader("digest:x"); err == nil {
		t.Error("an unknown secret type must be rejected")
	}
}

func TestAuthSecretFromKuboConfig(t *testing.T) {
	for _, tc := range []struct {
		name, api, want, wantErr string
	}{
		{name: "no API section", api: `{}`},
		{name: "open API", api: `{"API":{"HTTPHeaders":{}}}`},
		{name: "single entry", api: `{"API":{"Authorizations":{"webui":{"AuthSecret":"bearer:one"}}}}`, want: "bearer:one"},
		{name: "named entry wins", api: `{"API":{"Authorizations":{"webui":{"AuthSecret":"bearer:one"},"dlockss":{"AuthSecret":"bearer:two"}}}}`, want: "bearer:two"},
		{name: "ambiguous", api: `{"API":{"Authorizations":{"a":{"AuthSecret":"x"},"b":{"AuthSecret":"y"}}}}`, wantErr: `none named "dlockss"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config")
			if err := os.WriteFile(path, []byte(tc.api), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := AuthSecretFromKuboConfig(path)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want error containing %q, got %q, %v", tc.wantErr, got, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}
