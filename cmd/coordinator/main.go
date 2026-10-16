package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"
	"training-goodput-lab/internal/coordinator"
	"training-goodput-lab/internal/fault"
)

func main() {
	c := coordinator.Config{}
	l := coordinator.LiveConfig{}
	flag.StringVar(&l.Address, "listen", ":9000", "TCP listen address")
	flag.StringVar(&l.CSV, "csv", "live.csv", "event CSV output")
	flag.IntVar(&l.MaxSteps, "steps", 0, "stop after this many committed barrier steps (zero runs forever)")
	flag.IntVar(&c.Workers, "workers", 8, "active workers")
	flag.IntVar(&c.Spares, "spares", 2, "initial spare workers")
	flag.IntVar(&c.CheckpointEvery, "checkpoint-every", 50, "checkpoint interval in steps")
	flag.IntVar(&c.Window, "window", 5, "straggler observation window")
	flag.Float64Var(&c.StragglerMultiple, "straggler-multiple", 2, "duration multiple of worker median")
	flag.DurationVar(&c.StepTime, "step-time", 100*time.Millisecond, "nominal worker step duration")
	flag.DurationVar(&c.CheckpointCost, "checkpoint-cost", 500*time.Millisecond, "checkpoint write pause")
	flag.DurationVar(&c.SnapshotCost, "snapshot-cost", 10*time.Millisecond, "async snapshot pause")
	flag.DurationVar(&c.HeartbeatTimeout, "heartbeat-timeout", time.Second, "heartbeat timeout")
	flag.DurationVar(&c.ProgressTimeout, "progress-timeout", 2*time.Second, "per-step progress timeout")
	flag.DurationVar(&c.RecoveryTime, "recovery-time", 0, "modeled recovery delay")
	flag.BoolVar(&c.Async, "async", false, "background checkpoint writes")
	flag.BoolVar(&c.Watchdog, "watchdog", true, "enable progress watchdog")
	flag.DurationVar(&l.MTBFPerGPU, "mtbf-per-gpu", 0, "enable seeded injector with this per-worker MTBF (zero disables)")
	flag.Int64Var(&l.Seed, "seed", 42, "fault injector seed")
	l.FaultMix = fault.DefaultMix()
	flag.Float64Var(&l.FaultMix.Crash, "crash-weight", .7, "crash fault weight")
	flag.Float64Var(&l.FaultMix.Hang, "hang-weight", .15, "hang fault weight")
	flag.Float64Var(&l.FaultMix.Slow, "slow-weight", .15, "slow fault weight")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := coordinator.RunLive(ctx, c, l); err != nil {
		log.Fatal(err)
	}
}
