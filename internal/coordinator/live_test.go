package coordinator

import (
	"context"
	"encoding/csv"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"training-goodput-lab/internal/worker"
)

type registrationLog struct{ lines chan string }

func (w registrationLog) Write(p []byte) (int, error) {
	select {
	case w.lines <- string(p):
	default:
	}
	return len(p), nil
}

func TestLiveRecovery(t *testing.T) {
	for _, kind := range []string{"crash", "hang", "slow"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			csvPath := filepath.Join(dir, "live.csv")
			logs := registrationLog{lines: make(chan string, 100)}
			cfg := Config{Workers: 3, Spares: 1, CheckpointEvery: 3, Window: 2, StepTime: 3 * time.Millisecond, CheckpointCost: time.Millisecond, SnapshotCost: time.Millisecond, HeartbeatTimeout: 200 * time.Millisecond, ProgressTimeout: 80 * time.Millisecond, Watchdog: true, StragglerMultiple: 2}
			result := make(chan error, 1)
			go func() {
				result <- ServeLive(ctx, listener, cfg, LiveConfig{MaxSteps: 12, CSV: csvPath, Logger: log.New(logs, "", 0)})
			}()
			workerResults := make(chan error, 4)
			for _, id := range []string{"faulty", "healthy1", "healthy2", "spare"} {
				wc := worker.Config{Address: listener.Addr().String(), ID: id, Directory: dir, StepTime: cfg.StepTime, Heartbeat: 5 * time.Millisecond, BlobSize: 128, SlowFactor: 8, FaultAfter: 8}
				if id == "faulty" {
					wc.Fault = kind
				}
				go func() { workerResults <- worker.Run(ctx, wc) }()
				registered := false
				for !registered {
					select {
					case line := <-logs.lines:
						registered = strings.Contains(line, "registered worker="+id+" ")
					case <-ctx.Done():
						t.Fatal("registration timed out")
					}
				}
			}
			select {
			case err := <-result:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("training did not finish")
			}
			cancel()
			for range 4 {
				select {
				case <-workerResults:
				case <-time.After(time.Second):
					t.Fatal("worker did not stop")
				}
			}
			f, err := os.Open(csvPath)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			rows, err := csv.NewReader(f).ReadAll()
			if err != nil {
				t.Fatal(err)
			}
			var failed, rolled, recovered, finished bool
			for _, r := range rows[1:] {
				switch r[1] {
				case "failure":
					failed = r[4] == "faulty" && r[5] == kind
				case "rollback":
					rolled = r[2] == r[3] && r[6] == "1"
				case "recovered":
					recovered = true
				case "finished":
					finished = r[2] == "12"
				}
			}
			if !failed || !rolled || !recovered || !finished {
				t.Fatalf("missing correct recovery: failed=%v rollback=%v recovered=%v finished=%v rows=%v", failed, rolled, recovered, finished, rows)
			}
		})
	}
}

func TestLiveCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Workers: 1, CheckpointEvery: 1, Window: 1, StepTime: time.Millisecond, HeartbeatTimeout: time.Second, ProgressTimeout: time.Second, StragglerMultiple: 2}
	done := make(chan error, 1)
	go func() { done <- ServeLive(ctx, listener, cfg, LiveConfig{Logger: log.New(io.Discard, "", 0)}) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation blocked")
	}
}
