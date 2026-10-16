package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"training-goodput-lab/internal/checkpoint"
)

type Message struct {
	Type       string        `json:"type"`
	ID         string        `json:"id,omitempty"`
	Step       int           `json:"step,omitempty"`
	Generation int           `json:"generation,omitempty"`
	Duration   time.Duration `json:"duration,omitempty"`
	Path       string        `json:"path,omitempty"`
	Async      bool          `json:"async,omitempty"`
	Pause      time.Duration `json:"pause,omitempty"`
	Cost       time.Duration `json:"cost,omitempty"`
	Error      string        `json:"error,omitempty"`
}

type Config struct {
	Address, ID, Directory, Fault string
	StepTime, Heartbeat, DriftDay time.Duration
	BlobSize, FaultAfter          int
	SlowFactor, Drift             float64
}

func Run(ctx context.Context, cfg Config) error {
	if cfg.ID == "" {
		cfg.ID = fmt.Sprintf("worker-%d", os.Getpid())
	}
	if cfg.Heartbeat <= 0 || cfg.StepTime <= 0 || cfg.BlobSize < 0 {
		return errors.New("heartbeat and step time must be positive and blob size nonnegative")
	}
	if cfg.SlowFactor <= 0 {
		cfg.SlowFactor = 5
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", cfg.Address)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { <-ctx.Done(); conn.Close() }()
	encoder := json.NewEncoder(conn)
	var sendMu sync.Mutex
	send := func(m Message) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return err
		}
		return encoder.Encode(m)
	}
	if err := send(Message{Type: "register", ID: cfg.ID}); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(cfg.Heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if send(Message{Type: "heartbeat", ID: cfg.ID}) != nil {
					cancel()
					return
				}
			}
		}
	}()
	state := checkpoint.NewState(cfg.BlobSize)
	decoder := json.NewDecoder(conn)
	started := time.Now()
	faultReported := false
	for {
		var m Message
		if err := decoder.Decode(&m); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		switch m.Type {
		case "inject":
			cfg.Fault = m.Error
			cfg.FaultAfter = 0
			faultReported = false
		case "restore":
			if !pause(ctx, m.Pause) {
				return nil
			}
			restored, err := checkpoint.Load(m.Path)
			if err != nil {
				return fmt.Errorf("restore: %w", err)
			}
			state = restored
			if err := send(Message{Type: "restored", ID: cfg.ID, Step: state.Step, Generation: m.Generation}); err != nil {
				return err
			}
		case "step":
			faulted := cfg.Fault != "" && m.Step >= cfg.FaultAfter
			if faulted && !faultReported {
				if err := send(Message{Type: "fault", ID: cfg.ID, Error: cfg.Fault, Generation: m.Generation}); err != nil {
					return err
				}
				faultReported = true
			}
			if faulted && cfg.Fault == "crash" {
				return errors.New("injected crash")
			}
			if faulted && cfg.Fault == "hang" {
				continue
			}
			delay := cfg.StepTime + time.Duration((m.Step*17+len(cfg.ID)*13)%21-10)*cfg.StepTime/500
			if cfg.Drift > 0 && cfg.DriftDay > 0 {
				delay += time.Duration(float64(cfg.StepTime) * cfg.Drift * float64(time.Since(started)%cfg.DriftDay) / float64(cfg.DriftDay))
			}
			if faulted && cfg.Fault == "slow" {
				delay = time.Duration(float64(delay) * cfg.SlowFactor)
			}
			began := time.Now()
			if !pause(ctx, delay) {
				return nil
			}
			state.Advance(m.Step)
			if err := send(Message{Type: "done", ID: cfg.ID, Step: m.Step, Generation: m.Generation, Duration: time.Since(began)}); err != nil {
				return err
			}
		case "checkpoint":
			snapshot := state
			snapshot.Blob = append([]byte(nil), state.Blob...)
			if !pause(ctx, m.Pause) {
				return nil
			}
			write := func() {
				if !pause(ctx, m.Cost) {
					return
				}
				path := filepath.Join(cfg.Directory, cfg.ID, fmt.Sprintf("g%d-step%d.bin", m.Generation, m.Step))
				err := checkpoint.Save(path, snapshot)
				result := Message{Type: "checkpointed", ID: cfg.ID, Step: m.Step, Generation: m.Generation, Path: path}
				if err != nil {
					result.Error = err.Error()
				}
				if send(result) != nil {
					cancel()
				}
			}
			if m.Async {
				if err := send(Message{Type: "snapshotted", ID: cfg.ID, Step: m.Step, Generation: m.Generation}); err != nil {
					return err
				}
				go write()
			} else {
				write()
			}
		default:
			return fmt.Errorf("unknown command %q", m.Type)
		}
	}
}

func pause(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
