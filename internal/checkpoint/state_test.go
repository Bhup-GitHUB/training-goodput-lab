package checkpoint

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRestoreExactlyMatchesCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker", "state.bin")
	original := NewState(1024)
	original.Advance(47)
	if err := Save(path, original); err != nil {
		t.Fatal(err)
	}
	original.Advance(93)
	restored, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	expected := NewState(1024)
	expected.Advance(47)
	if !reflect.DeepEqual(restored, expected) {
		t.Fatal("restored state differs from checkpoint")
	}
	for step := 48; step <= 100; step++ {
		restored.Advance(step)
		expected.Advance(step)
	}
	if !reflect.DeepEqual(restored, expected) {
		t.Fatal("replayed state differs from uninterrupted state")
	}
}

func TestLoadRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.bin")
	if err := Save(path, NewState(64)); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[25] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("corrupt checkpoint accepted")
	}
}

func TestLoadRejectsTruncatedCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.bin")
	if err := os.WriteFile(path, []byte("TGLAB001"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("truncated checkpoint accepted")
	}
}
