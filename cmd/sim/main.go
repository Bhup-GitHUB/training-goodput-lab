package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"
	"training-goodput-lab/internal/charts"
	"training-goodput-lab/internal/coordinator"
	"training-goodput-lab/internal/fault"
	"training-goodput-lab/internal/metrics"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	c := coordinator.DefaultConfig()
	c.Workers = 16384
	flag.IntVar(&c.Workers, "gpus", c.Workers, "Active simulated GPUs")
	flag.IntVar(&c.Spares, "spares", c.Spares, "Available replacements")
	flag.IntVar(&c.CheckpointEvery, "checkpoint-every", c.CheckpointEvery, "Steps between checkpoints")
	flag.IntVar(&c.Window, "straggler-window", c.Window, "Duration samples before replacing a straggler")
	flag.Float64Var(&c.StragglerMultiple, "straggler-multiple", c.StragglerMultiple, "Slow threshold relative to cohort median")
	flag.DurationVar(&c.StepTime, "step-time", c.StepTime, "Healthy step duration")
	flag.DurationVar(&c.CheckpointCost, "checkpoint-cost", c.CheckpointCost, "Checkpoint write duration")
	flag.DurationVar(&c.SnapshotCost, "snapshot-cost", c.SnapshotCost, "Async snapshot pause")
	flag.DurationVar(&c.HeartbeatTimeout, "heartbeat-timeout", c.HeartbeatTimeout, "Crash detection timeout")
	flag.DurationVar(&c.ProgressTimeout, "progress-timeout", c.ProgressTimeout, "Silent hang watchdog deadline")
	flag.DurationVar(&c.RecoveryTime, "recovery-time", c.RecoveryTime, "Restore duration")
	flag.BoolVar(&c.Async, "async", false, "Write checkpoints in background")
	flag.BoolVar(&c.Watchdog, "watchdog", true, "Detect missing step progress")
	days := flag.Float64("days", 54, "Simulation duration in days")
	seed := flag.Int64("seed", 42, "Fault and timing random seed")
	preset := flag.String("preset", "llama3", "Failure rate preset: llama3")
	mtbf := flag.Duration("gpu-mtbf", fault.Llama3MTBF(), "Per-GPU mean time between faults")
	mix := fault.DefaultMix()
	flag.Float64Var(&mix.Crash, "crash-weight", mix.Crash, "Relative crash frequency")
	flag.Float64Var(&mix.Hang, "hang-weight", mix.Hang, "Relative silent hang frequency")
	flag.Float64Var(&mix.Slow, "slow-weight", mix.Slow, "Relative straggler frequency")
	drift := flag.Float64("drift", 0, "Daily sinusoidal fractional step-time drift")
	output := flag.String("output", "results/sim", "Output directory")
	flag.Parse()
	if *preset != "llama3" {
		return fmt.Errorf("unknown preset %q", *preset)
	}
	if math.IsNaN(*days) || math.IsInf(*days, 0) || *days <= 0 || *days > float64(math.MaxInt64)/float64(24*time.Hour) {
		return fmt.Errorf("days must be positive and fit in a duration")
	}
	r, err := coordinator.RunSimulation(coordinator.SimConfig{Config: c, Duration: time.Duration(*days * float64(24*time.Hour)), GPUMTBF: *mtbf, Seed: *seed, Mix: mix, Drift: *drift})
	if err != nil {
		return err
	}
	metrics.PrintSummary(os.Stdout, r)
	if err := metrics.WriteCSV(filepath.Join(*output, "run.csv"), r); err != nil {
		return err
	}
	return charts.RenderTimeline(filepath.Join(*output, "timeline.png"), r.Timeline)
}
