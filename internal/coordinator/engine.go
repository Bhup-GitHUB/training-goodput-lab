package coordinator

import (
	"fmt"
	"math"
	"sort"
	"time"
	"training-goodput-lab/internal/fault"
)

type Config struct {
	Workers, Spares, CheckpointEvery, Window                                                int
	StepTime, CheckpointCost, SnapshotCost, HeartbeatTimeout, ProgressTimeout, RecoveryTime time.Duration
	Async, Watchdog                                                                         bool
	StragglerMultiple                                                                       float64
}

func DefaultConfig() Config {
	return Config{Workers: 8, Spares: 1000, CheckpointEvery: 50, Window: 5, StepTime: 30 * time.Second, CheckpointCost: 120 * time.Second, SnapshotCost: 2 * time.Second, HeartbeatTimeout: 10 * time.Second, ProgressTimeout: 90 * time.Second, RecoveryTime: 60 * time.Second, Watchdog: true, StragglerMultiple: 2}
}
func (c Config) Validate() error {
	if c.Workers < 1 || c.Spares < 0 || c.CheckpointEvery < 1 || c.Window < 1 {
		return fmt.Errorf("workers, checkpoint interval and window must be positive; spares must be nonnegative")
	}
	if c.StepTime <= 0 || c.CheckpointCost < 0 || c.SnapshotCost < 0 || c.HeartbeatTimeout <= 0 || c.ProgressTimeout <= 0 || c.RecoveryTime < 0 {
		return fmt.Errorf("invalid timing configuration")
	}
	if math.IsNaN(c.StragglerMultiple) || math.IsInf(c.StragglerMultiple, 0) || c.StragglerMultiple <= 1 {
		return fmt.Errorf("straggler multiple must be finite and greater than one")
	}
	return nil
}

type Engine struct {
	Config                                                  Config
	Step, CheckpointStep, Generation, SparesUsed, LostSteps int
	durations                                               map[string][]time.Duration
}

func NewEngine(c Config) *Engine {
	return &Engine{Config: c, durations: make(map[string][]time.Duration)}
}
func (e *Engine) CompleteStep() bool { e.Step++; return e.Step%e.Config.CheckpointEvery == 0 }
func (e *Engine) CommitCheckpoint(step int) {
	if step >= e.CheckpointStep && step <= e.Step {
		e.CheckpointStep = step
	}
}
func (e *Engine) Recover(workerID string, kind fault.Kind, at time.Duration) error {
	if e.SparesUsed >= e.Config.Spares {
		return fmt.Errorf("spares exhausted at %s after %s on %s", at, kind, workerID)
	}
	e.LostSteps += e.Step - e.CheckpointStep
	e.Step = e.CheckpointStep
	e.Generation++
	e.SparesUsed++
	e.durations = make(map[string][]time.Duration)
	return nil
}
func (e *Engine) Observe(worker string, d time.Duration) bool {
	h := append(e.durations[worker], d)
	if len(h) > e.Config.Window {
		h = h[1:]
	}
	e.durations[worker] = h
	if len(h) < e.Config.Window || len(e.durations) < 2 {
		return false
	}
	means := make([]float64, 0, len(e.durations))
	var own float64
	for id, history := range e.durations {
		if len(history) < e.Config.Window {
			continue
		}
		var sum float64
		for _, v := range history {
			sum += float64(v)
		}
		mean := sum / float64(len(history))
		means = append(means, mean)
		if id == worker {
			own = mean
		}
	}
	if len(means) < 2 {
		return false
	}
	sort.Float64s(means)
	median := means[len(means)/2]
	if len(means)%2 == 0 {
		median = (means[len(means)/2-1] + median) / 2
	}
	return own > median*e.Config.StragglerMultiple
}

func (e *Engine) ProgressExpired(elapsed time.Duration) bool {
	return e.Config.Watchdog && elapsed >= e.Config.ProgressTimeout
}
func (e *Engine) HeartbeatExpired(elapsed time.Duration) bool {
	return elapsed >= e.Config.HeartbeatTimeout
}
