package coordinator

import (
	"fmt"
	"math"
	"math/rand"
	"time"
	"training-goodput-lab/internal/clock"
	"training-goodput-lab/internal/fault"
	"training-goodput-lab/internal/metrics"
)

type SimConfig struct {
	Config            Config
	Duration, GPUMTBF time.Duration
	Seed              int64
	Mix               fault.Mix
	Drift             float64
}

type simulatedFault struct {
	index  int
	worker string
	kind   fault.Kind
	detect time.Duration
}

func RunSimulation(s SimConfig) (metrics.Run, error) {
	r := metrics.Run{Workers: s.Config.Workers, Seed: s.Seed}
	if err := s.Config.Validate(); err != nil {
		return r, err
	}
	if s.Duration <= 0 || s.GPUMTBF <= 0 {
		return r, fmt.Errorf("duration and per-GPU MTBF must be positive")
	}
	if err := s.Mix.Validate(); err != nil {
		return r, err
	}
	if math.IsNaN(s.Drift) || s.Drift < 0 || s.Drift >= .5 {
		return r, fmt.Errorf("drift must be between zero and 0.5")
	}
	c := s.Config
	e := NewEngine(c)
	rng := rand.New(rand.NewSource(s.Seed))
	timingRNG := rand.New(rand.NewSource(s.Seed ^ 0x5deece66d))
	v := clock.Virtual{}
	const never = time.Duration(math.MaxInt64)
	jobMTBF := s.GPUMTBF / time.Duration(c.Workers)
	if jobMTBF < time.Nanosecond {
		return r, fmt.Errorf("cluster failure rate exceeds clock resolution")
	}
	nextFault := fault.Next(rng, jobMTBF)
	pending := []simulatedFault{}
	stepStart := time.Duration(0)
	base := c.StepTime
	stepEnd := base
	stepWatchdog := never
	pauseEnd := never
	recoveryEnd := never
	writeEnd := never
	writeStep := 0
	checkpointPause := false
	slowSteps := 0
	useful := time.Duration(0)
	committedUseful := time.Duration(0)
	writeUseful := time.Duration(0)
	addPoint := func(event string) {
		r.Timeline = append(r.Timeline, metrics.Point{Time: v.Now(), Step: e.Step, Event: event})
	}
	addPoint("start")
	startStep := func() {
		base = time.Duration(float64(c.StepTime) * (1 + s.Drift*math.Sin(2*math.Pi*v.Now().Hours()/24)))
		base += time.Duration(timingRNG.Float64() * .02 * float64(c.StepTime))
		stepStart = v.Now()
		stepEnd = stepStart + base
		stepWatchdog = never
		if c.Watchdog {
			stepWatchdog = stepStart + c.ProgressTimeout
		}
		for i := range pending {
			p := &pending[i]
			if p.kind == fault.Hang && c.Watchdog {
				p.detect = stepStart + c.ProgressTimeout
			}
			if p.kind == fault.Crash || p.kind == fault.Hang {
				stepEnd = never
			}
			if p.kind == fault.Slow && stepEnd != never {
				stepEnd = stepStart + 4*base
			}
		}
	}
	startStep()
	for v.Now() < s.Duration {
		eventAt := min(s.Duration, nextFault, stepEnd, pauseEnd, recoveryEnd, writeEnd, stepWatchdog)
		for _, p := range pending {
			eventAt = min(eventAt, p.detect)
		}
		elapsed := eventAt - v.Now()
		if checkpointPause {
			r.CheckpointTime += elapsed
		}
		v.Advance(elapsed)
		if v.Now() == s.Duration {
			break
		}
		if v.Now() == writeEnd {
			e.CommitCheckpoint(writeStep)
			committedUseful = writeUseful
			writeEnd = never
			addPoint("checkpoint_committed")
		}
		if v.Now() == nextFault {
			kind := s.Mix.Sample(rng)
			worker := fmt.Sprintf("gpu-%d", rng.Intn(c.Workers))
			r.Failures = append(r.Failures, metrics.Failure{Kind: kind, At: v.Now()})
			p := simulatedFault{index: len(r.Failures) - 1, worker: worker, kind: kind, detect: never}
			if kind == fault.Crash {
				p.detect = v.Now() + c.HeartbeatTimeout
			}
			if kind == fault.Hang && c.Watchdog && pauseEnd == never && recoveryEnd == never {
				p.detect = max(v.Now(), stepStart+c.ProgressTimeout)
			}
			pending = append(pending, p)
			addPoint("failure_" + string(kind))
			if recoveryEnd == never && pauseEnd == never {
				if kind == fault.Slow && stepEnd != never {
					stepEnd = max(stepEnd, stepStart+4*base)
				} else if kind != fault.Slow {
					stepEnd = never
				}
			}
			nextFault = v.Now() + max(time.Nanosecond, fault.Next(rng, jobMTBF))
		}
		if v.Now() == stepWatchdog {
			stepWatchdog = never
			if stepEnd > v.Now() {
				if len(pending) == 0 {
					return r, fmt.Errorf("progress timeout shorter than healthy step duration")
				}
				pending[0].detect = v.Now()
			}
		}
		detected := -1
		for i, p := range pending {
			if p.detect <= v.Now() {
				detected = i
				break
			}
		}
		if detected >= 0 {
			p := pending[detected]
			before := e.Step
			if err := e.Recover(p.worker, p.kind, v.Now()); err != nil {
				return r, err
			}
			f := &r.Failures[p.index]
			f.Detected = true
			f.Detection = v.Now() - f.At
			f.Recovery = c.RecoveryTime
			f.LostSteps = before - e.Step
			pending = append(pending[:detected], pending[detected+1:]...)
			useful = committedUseful
			stepWatchdog = never
			slowSteps = 0
			stepEnd = never
			pauseEnd = never
			checkpointPause = false
			writeEnd = never
			recoveryEnd = v.Now() + c.RecoveryTime
			addPoint("rollback_" + string(p.kind))
		}
		if v.Now() == recoveryEnd {
			recoveryEnd = never
			addPoint("recovered")
			startStep()
		}
		if v.Now() == pauseEnd {
			pauseEnd = never
			checkpointPause = false
			startStep()
		}
		if v.Now() == stepEnd {
			useful += base
			stepWatchdog = never
			due := e.CompleteStep()
			addPoint("step")
			var slow *simulatedFault
			for i := range pending {
				if pending[i].kind == fault.Slow {
					slow = &pending[i]
					break
				}
			}
			if slow != nil {
				slowSteps++
				e.Observe("healthy-a", base)
				e.Observe("healthy-b", base)
				if e.Observe(slow.worker, v.Now()-stepStart) && slowSteps >= c.Window {
					slow.detect = v.Now()
				}
			}
			stepEnd = never
			if due && writeEnd == never {
				writeStep = e.Step
				writeUseful = useful
				checkpointPause = true
				if c.Async {
					pauseEnd = v.Now() + c.SnapshotCost
					writeEnd = pauseEnd + c.CheckpointCost
				} else {
					pauseEnd = v.Now() + c.CheckpointCost
					writeEnd = pauseEnd
				}
				addPoint("checkpoint_started")
			} else {
				startStep()
			}
		}
	}
	for _, p := range pending {
		r.Failures[p.index].Detection = s.Duration - r.Failures[p.index].At
	}
	r.Steps = e.Step
	r.LostSteps = e.LostSteps
	r.SparesUsed = e.SparesUsed
	r.WallTime = v.Now()
	r.UsefulTime = useful
	addPoint("end")
	if r.UsefulTime > r.WallTime {
		return r, fmt.Errorf("useful training time exceeds elapsed time")
	}
	return r, nil
}
