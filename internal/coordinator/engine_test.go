package coordinator

import (
	"reflect"
	"testing"
	"time"
	"training-goodput-lab/internal/fault"
)

func TestRecoveryRollsBackAndPromotesSpare(t *testing.T) {
	c := DefaultConfig()
	e := NewEngine(c)
	for range 55 {
		if e.CompleteStep() {
			e.CommitCheckpoint(e.Step)
		}
	}
	if err := e.Recover("gpu-3", fault.Crash, time.Minute); err != nil {
		t.Fatal(err)
	}
	if e.Step != 50 || e.LostSteps != 5 || e.SparesUsed != 1 || e.Generation != 1 {
		t.Fatalf("unexpected recovery: %+v", e)
	}
	e.CompleteStep()
	if e.Step != 51 {
		t.Fatalf("resumed at %d", e.Step)
	}
}
func TestPersistentStraggler(t *testing.T) {
	c := DefaultConfig()
	e := NewEngine(c)
	for i := 0; i < c.Window; i++ {
		e.Observe("a", time.Second)
		e.Observe("b", time.Second)
		slow := e.Observe("c", 4*time.Second)
		if slow != (i == c.Window-1) {
			t.Fatalf("straggler detection at sample %d: %v", i, slow)
		}
	}
}
func simTestConfig() SimConfig {
	c := DefaultConfig()
	c.Workers = 64
	c.StepTime = time.Second
	c.CheckpointEvery = 10
	c.CheckpointCost = 2 * time.Second
	c.SnapshotCost = 100 * time.Millisecond
	c.ProgressTimeout = 5 * time.Second
	c.HeartbeatTimeout = 2 * time.Second
	c.RecoveryTime = time.Second
	return SimConfig{Config: c, Duration: time.Hour, GPUMTBF: 64 * time.Minute, Seed: 42, Mix: fault.DefaultMix()}
}
func TestSimulationDeterministic(t *testing.T) {
	s := simTestConfig()
	a, err := RunSimulation(s)
	if err != nil {
		t.Fatal(err)
	}
	b, err := RunSimulation(s)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatal("same seed changed results")
	}
	if len(a.Failures) == 0 || a.SparesUsed == 0 || a.Steps == 0 {
		t.Fatalf("simulation did not exercise recovery: %+v", a)
	}
}
func TestHangWatchdogAndHeartbeatOnly(t *testing.T) {
	s := simTestConfig()
	s.Mix = fault.Mix{Hang: 1}
	a, err := RunSimulation(s)
	if err != nil {
		t.Fatal(err)
	}
	detected := 0
	for _, f := range a.Failures {
		if f.Detected {
			detected++
			if f.Detection > s.Config.ProgressTimeout {
				t.Fatalf("watchdog delayed %s", f.Detection)
			}
		}
	}
	if detected == 0 {
		t.Fatal("no hangs detected")
	}
	s.Config.Watchdog = false
	b, err := RunSimulation(s)
	if err != nil {
		t.Fatal(err)
	}
	if b.SparesUsed != 0 {
		t.Fatal("heartbeats should not detect silent hangs")
	}
	if b.Goodput() >= a.Goodput() {
		t.Fatal("watchdog did not improve goodput")
	}
	for _, f := range b.Failures {
		if f.Detected {
			t.Fatal("hang marked detected")
		}
	}
}
func TestGoodputBounded(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, drift := range []float64{0, .2} {
			for seed := int64(0); seed < 10; seed++ {
				s := simTestConfig()
				s.Config.Async = async
				s.Drift = drift
				s.Seed = seed
				r, err := RunSimulation(s)
				if err != nil {
					t.Fatal(err)
				}
				if r.UsefulTime > r.WallTime || r.Goodput() < 0 || r.Goodput() > 1 {
					t.Fatalf("invalid goodput %v", r.Goodput())
				}
			}
		}
	}
}
func TestSimulationStragglerReplaced(t *testing.T) {
	s := simTestConfig()
	s.Mix = fault.Mix{Slow: 1}
	r, err := RunSimulation(s)
	if err != nil {
		t.Fatal(err)
	}
	if r.SparesUsed == 0 {
		t.Fatal("persistent straggler never replaced")
	}
}
