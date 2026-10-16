package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	gpus := flag.Int("gpus", 16384, "Active workers")
	days := flag.Float64("days", 54, "Simulation duration in days")
	seed := flag.Int64("seed", 42, "Random seed")
	preset := flag.String("preset", "llama3", "Failure rate preset: llama3")
	out := flag.String("output", "results", "CSV and chart directory")
	intervals := flag.String("intervals", "1,5,10,20,35,50,75,100,200,400,800", "Checkpoint intervals in steps")
	sizes := flag.String("sizes", "256,1024,4096,8192,16384,32768", "Cluster sizes")
	cost := flag.Duration("checkpoint-cost", 120*time.Second, "Checkpoint write duration")
	step := flag.Duration("step-time", 30*time.Second, "Useful step duration")
	every := flag.Int("checkpoint-every", 50, "Interval for strategy and scale comparisons")
	mtbf := flag.Duration("gpu-mtbf", fault.Llama3MTBF(), "Per-GPU MTBF; overrides preset rate")
	flag.Parse()
	if *preset != "llama3" {
		return fmt.Errorf("unknown preset %q", *preset)
	}
	if *days <= 0 || math.IsNaN(*days) || math.IsInf(*days, 0) || *days > float64(math.MaxInt64)/float64(24*time.Hour) {
		return fmt.Errorf("days must be positive and fit in a duration")
	}
	ks, err := integers(*intervals)
	if err != nil {
		return err
	}
	ns, err := integers(*sizes)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*out, 0755); err != nil {
		return err
	}
	cfg := coordinator.DefaultConfig()
	cfg.Workers = *gpus
	cfg.Spares = 100000
	cfg.CheckpointCost = *cost
	cfg.StepTime = *step
	cfg.CheckpointEvery = *every
	if err := cfg.Validate(); err != nil {
		return err
	}
	if *mtbf <= 0 {
		return fmt.Errorf("gpu-mtbf must be positive")
	}
	duration := time.Duration(*days * float64(24*time.Hour))
	base := coordinator.SimConfig{Config: cfg, Duration: duration, GPUMTBF: *mtbf, Seed: *seed, Mix: fault.DefaultMix()}
	rows := [][]string{{"experiment", "parameter", "goodput", "steps", "lost_steps", "failures", "spares_used", "checkpoint_seconds"}}
	execute := func(name string, param string, c coordinator.SimConfig) (metrics.Run, error) {
		result, err := coordinator.RunSimulation(c)
		if err != nil {
			return result, fmt.Errorf("%s: %w", name, err)
		}
		if err := metrics.WriteCSV(filepath.Join(*out, name+".csv"), result); err != nil {
			return result, err
		}
		fmt.Printf("\n%s\n", name)
		metrics.PrintSummary(os.Stdout, result)
		rows = append(rows, []string{name, param, strconv.FormatFloat(result.Goodput(), 'f', 9, 64), strconv.Itoa(result.Steps), strconv.Itoa(result.LostSteps), strconv.Itoa(len(result.Failures)), strconv.Itoa(result.SparesUsed), strconv.FormatFloat(result.CheckpointTime.Seconds(), 'f', 3, 64)})
		return result, nil
	}
	checkpointSeries := []charts.Series{{Name: "Synchronous checkpoints"}, {Name: "Asynchronous checkpoints"}}
	bestK := 0
	best := -1.0
	for _, k := range ks {
		for i := 0; i < 2; i++ {
			c := base
			c.Config.CheckpointEvery = k
			c.Config.Async = i == 1
			name := fmt.Sprintf("checkpoint_%s_%d", []string{"sync", "async"}[i], k)
			result, err := execute(name, strconv.Itoa(k), c)
			if err != nil {
				return err
			}
			checkpointSeries[i].Points = append(checkpointSeries[i].Points, charts.XY{X: float64(k), Y: result.Goodput()})
			if i == 0 && result.Goodput() > best {
				best = result.Goodput()
				bestK = k
			}
		}
	}
	predicted := math.Sqrt(2*cfg.CheckpointCost.Seconds()*(base.GPUMTBF.Seconds()/float64(cfg.Workers))) / cfg.StepTime.Seconds()
	fmt.Printf("\nYoung first-order prediction: %.1f steps; measured best sync interval: %d steps (%.2f%% goodput)\n", predicted, bestK, 100*best)
	if err := charts.RenderLines(filepath.Join(*out, "goodput_vs_checkpoint_interval.png"), fmt.Sprintf("Checkpoint tradeoff: measured sync best K=%d; Young K=%.1f", bestK, predicted), "Checkpoint interval (steps)", "Goodput", checkpointSeries, predicted); err != nil {
		return err
	}
	scale := charts.Series{Name: fmt.Sprintf("K=%d, fixed per-GPU MTBF", *every)}
	for _, n := range ns {
		c := base
		c.Config.Workers = n
		result, err := execute(fmt.Sprintf("scale_%d", n), strconv.Itoa(n), c)
		if err != nil {
			return err
		}
		scale.Points = append(scale.Points, charts.XY{X: float64(n), Y: result.Goodput()})
	}
	if err := charts.RenderLines(filepath.Join(*out, "goodput_vs_cluster_size.png"), "Larger synchronous jobs encounter more failures", "Active GPUs", "Goodput", []charts.Series{scale}, 0); err != nil {
		return err
	}
	hangs := base
	hangs.Mix = fault.Mix{Hang: 1}
	watchdog, err := execute("hang_watchdog", "enabled", hangs)
	if err != nil {
		return err
	}
	hangs.Config.Watchdog = false
	heartbeat, err := execute("hang_heartbeats_only", "disabled", hangs)
	if err != nil {
		return err
	}
	detection := func(run metrics.Run) (float64, bool) {
		var total float64
		n := 0
		censored := false
		for _, f := range run.Failures {
			if f.Kind == fault.Hang {
				total += f.Detection.Seconds()
				n++
				if !f.Detected {
					censored = true
				}
			}
		}
		if n == 0 {
			return 0, true
		}
		return total / float64(n), censored
	}
	wd, _ := detection(watchdog)
	hb, censored := detection(heartbeat)
	if err := charts.RenderDetection(filepath.Join(*out, "hang_detection_time.png"), wd, hb, censored); err != nil {
		return err
	}
	short := base
	short.Duration = 6 * time.Hour
	timeline, err := execute("timeline", "6h", short)
	if err != nil {
		return err
	}
	if err := charts.RenderTimeline(filepath.Join(*out, "timeline.png"), timeline.Timeline); err != nil {
		return err
	}
	return writeRows(filepath.Join(*out, "experiments.csv"), rows)
}

func integers(value string) ([]int, error) {
	var result []int
	for _, part := range strings.Split(value, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 1 {
			return nil, fmt.Errorf("invalid positive integer %q", part)
		}
		result = append(result, n)
	}
	return result, nil
}

func writeRows(path string, rows [][]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	err = w.WriteAll(rows)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}
