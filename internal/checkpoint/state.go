package checkpoint

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
)

type State struct {
	Step int
	Loss float64
	Blob []byte
}

func NewState(size int) State {
	if size < 0 {
		size = 0
	}
	state := State{Blob: make([]byte, size)}
	state.Advance(0)
	return state
}

func (s *State) Advance(step int) {
	s.Step = step
	s.Loss = 1 / (1 + float64(step)*0.001)
	seed := uint64(step) ^ 0x9e3779b97f4a7c15
	for index := range s.Blob {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		s.Blob[index] = byte(seed)
	}
}

func Save(path string, state State) error {
	if state.Step < 0 || math.IsNaN(state.Loss) || math.IsInf(state.Loss, 0) {
		return errors.New("invalid checkpoint state")
	}
	payload := make([]byte, 24+len(state.Blob))
	copy(payload, "TGLAB001")
	binary.LittleEndian.PutUint64(payload[8:16], uint64(state.Step))
	binary.LittleEndian.PutUint64(payload[16:24], math.Float64bits(state.Loss))
	copy(payload[24:], state.Blob)
	checksum := sha256.Sum256(payload)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create checkpoint directory: %w", err)
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".checkpoint-*")
	if err != nil {
		return fmt.Errorf("create checkpoint: %w", err)
	}
	temporaryPath := file.Name()
	defer os.Remove(temporaryPath)
	if _, err = file.Write(payload); err == nil {
		_, err = file.Write(checksum[:])
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close checkpoint: %w", closeErr)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish checkpoint: %w", err)
	}
	return nil
}

func Load(path string) (State, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return State{}, fmt.Errorf("read checkpoint: %w", err)
	}
	if len(data) < 24+sha256.Size || string(data[:8]) != "TGLAB001" {
		return State{}, errors.New("invalid checkpoint format")
	}
	payload := data[:len(data)-sha256.Size]
	checksum := sha256.Sum256(payload)
	if !bytes.Equal(checksum[:], data[len(payload):]) {
		return State{}, errors.New("checkpoint checksum mismatch")
	}
	step := binary.LittleEndian.Uint64(payload[8:16])
	loss := math.Float64frombits(binary.LittleEndian.Uint64(payload[16:24]))
	if step > uint64(^uint(0)>>1) || math.IsNaN(loss) || math.IsInf(loss, 0) {
		return State{}, errors.New("invalid checkpoint state")
	}
	blob := make([]byte, len(payload)-24)
	copy(blob, payload[24:])
	return State{Step: int(step), Loss: loss, Blob: blob}, nil
}
