package metrics

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
	"training-goodput-lab/internal/fault"
)

type Failure struct {
	Kind                    fault.Kind
	At, Detection, Recovery time.Duration
	LostSteps               int
	Detected                bool
}
type Point struct {
	Time  time.Duration
	Step  int
	Event string
}
type Run struct {
	Workers                              int
	Seed                                 int64
	WallTime, UsefulTime, CheckpointTime time.Duration
	Steps, LostSteps, SparesUsed         int
	Failures                             []Failure
	Timeline                             []Point
}

func (r Run) Goodput() float64 {
	if r.WallTime <= 0 {
		return 0
	}
	return min(1, max(0, float64(r.UsefulTime)/float64(r.WallTime)))
}
func WriteCSV(path string, r Run) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	rows := [][]string{{"record", "time_seconds", "step", "event", "detection_seconds", "recovery_seconds", "lost_steps", "goodput", "workers", "seed", "spares_used", "checkpoint_seconds", "useful_time_seconds", "detected", "censored_elapsed_seconds", "failures", "detected_failures", "undetected_failures", "wall_time_seconds"}}
	detected := 0
	for _, failure := range r.Failures {
		if failure.Detected {
			detected++
		}
	}
	rows = append(rows, []string{"summary", seconds(r.WallTime), strconv.Itoa(r.Steps), "", "", "", strconv.Itoa(r.LostSteps), strconv.FormatFloat(r.Goodput(), 'f', 9, 64), strconv.Itoa(r.Workers), strconv.FormatInt(r.Seed, 10), strconv.Itoa(r.SparesUsed), seconds(r.CheckpointTime), seconds(r.UsefulTime), "", "", strconv.Itoa(len(r.Failures)), strconv.Itoa(detected), strconv.Itoa(len(r.Failures) - detected), seconds(r.WallTime)})
	for _, p := range r.Timeline {
		row := make([]string, len(rows[0]))
		row[0], row[1], row[2], row[3] = "timeline", seconds(p.Time), strconv.Itoa(p.Step), p.Event
		rows = append(rows, row)
	}
	for _, e := range r.Failures {
		row := make([]string, len(rows[0]))
		row[0], row[1], row[3], row[6], row[13] = "failure", seconds(e.At), string(e.Kind), strconv.Itoa(e.LostSteps), strconv.FormatBool(e.Detected)
		if e.Detected {
			row[4], row[5] = seconds(e.Detection), seconds(e.Recovery)
		} else {
			row[14] = seconds(e.Detection)
		}
		rows = append(rows, row)
	}

	err = w.WriteAll(rows)
	if err == nil {
		err = w.Error()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}
func seconds(t time.Duration) string { return strconv.FormatFloat(t.Seconds(), 'f', 6, 64) }
func PrintSummary(w io.Writer, r Run) {
	fmt.Fprintln(w, "workers  steps  goodput  failures  lost_steps  spares  checkpoint_s  wall_s")
	fmt.Fprintf(w, "%7d %6d %7.2f%% %9d %11d %7d %13.2f %9.2f\n", r.Workers, r.Steps, 100*r.Goodput(), len(r.Failures), r.LostSteps, r.SparesUsed, r.CheckpointTime.Seconds(), r.WallTime.Seconds())
	fmt.Fprintln(w, "fault   count  detected  undetected_censored  mean_detect_s  mean_recover_s  mean_lost_steps")
	for _, kind := range []fault.Kind{fault.Crash, fault.Hang, fault.Slow} {
		count, detected := 0, 0
		var detection, recovery time.Duration
		lost := 0
		for _, failure := range r.Failures {
			if failure.Kind != kind {
				continue
			}
			count++
			if failure.Detected {
				detected++
				detection += failure.Detection
				recovery += failure.Recovery
				lost += failure.LostSteps
			}
		}
		if count == 0 {
			continue
		}
		if detected == 0 {
			fmt.Fprintf(w, "%-7s %5d %9d %20d %14s %15s %16s\n", kind, count, detected, count-detected, "-", "-", "-")
			continue
		}
		fmt.Fprintf(w, "%-7s %5d %9d %20d %14.3f %15.3f %16.2f\n", kind, count, detected, count-detected, detection.Seconds()/float64(detected), recovery.Seconds()/float64(detected), float64(lost)/float64(detected))
	}
}
