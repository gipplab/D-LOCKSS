package fileops

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// uploadFile writes content to a temporary file and returns its path.
func uploadFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "paper.pdf")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestIngestUpload(t *testing.T) {
	w, kubo, shard := newPinRig(t)
	payload := dagPB(t, "paper")
	kubo.imported = payload

	got, err := w.fp.IngestUpload(context.Background(), uploadFile(t, "pipeline-content"), "paper.pdf", payload, alice, []byte{7})
	if err != nil {
		t.Fatal(err)
	}
	if manifests := kubo.manifestPins(); len(manifests) != 1 || !manifests[0].Equals(got) {
		t.Fatalf("want the upload's manifest %s pinned, got %v", got, manifests)
	}
	ro := kubo.manifest(t, got)
	if !ro.Payload.Equals(payload) || ro.Uploader != alice || !bytes.Equal(ro.UploaderSig, []byte{7}) ||
		ro.MetadataRef != "paper.pdf" || ro.TotalSize != 16 {
		t.Fatalf("unexpected manifest %+v", ro)
	}
	if shard.published != 1 {
		t.Fatalf("the upload should be announced once, got %d", shard.published)
	}
}

func TestIngestUpload_RejectsOtherCID(t *testing.T) {
	w, kubo, _ := newPinRig(t)
	kubo.imported = dagPB(t, "actual")

	_, err := w.fp.IngestUpload(context.Background(), uploadFile(t, "x"), "paper.pdf", dagPB(t, "signed"), alice, []byte{7})
	if !errors.Is(err, ErrPayloadMismatch) {
		t.Fatalf("want ErrPayloadMismatch, got %v", err)
	}
	if n := len(kubo.manifestPins()); n != 0 {
		t.Fatalf("a file with another CID must not be ingested, got %d manifests", n)
	}
}
