package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"
	"training-goodput-lab/internal/worker"
)

func main() {
	var c worker.Config
	flag.StringVar(&c.Address, "coordinator", "localhost:9000", "coordinator address")
	flag.StringVar(&c.ID, "id", "", "worker identifier (default process ID)")
	flag.StringVar(&c.Directory, "checkpoint-dir", ".training-goodput", "shared checkpoint directory")
	flag.DurationVar(&c.StepTime, "step-time", 100*time.Millisecond, "base step duration")
	flag.DurationVar(&c.Heartbeat, "heartbeat", 200*time.Millisecond, "heartbeat interval")
	flag.IntVar(&c.BlobSize, "blob-bytes", 4096, "fake state size")
	flag.StringVar(&c.Fault, "fault", "", "crash, hang, or slow")
	flag.IntVar(&c.FaultAfter, "fault-after", 20, "first step with injected fault")
	flag.Float64Var(&c.SlowFactor, "slow-factor", 5, "slow fault duration multiplier")
	flag.Float64Var(&c.Drift, "drift", 0, "fractional drift per day")
	flag.DurationVar(&c.DriftDay, "drift-day", 24*time.Hour, "duration of drift cycle")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := worker.Run(ctx, c); err != nil {
		log.Fatal(err)
	}
}
