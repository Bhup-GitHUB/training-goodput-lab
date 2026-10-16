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
	rows := [][]string{{"record", "time_seconds", "step", "event", "detection_seconds", "recovery_seconds", "lost_steps", "goodput"}, {"summary", seconds(r.WallTime), strconv.Itoa(r.Steps), "", "", "", strconv.Itoa(r.LostSteps), strconv.FormatFloat(r.Goodput(), 'f', 9, 64)}}
	for _, p := range r.Timeline {
		rows = append(rows, []string{"timeline", seconds(p.Time), strconv.Itoa(p.Step), p.Event, "", "", "", ""})
	}
	for _, e := range r.Failures {
		rows = append(rows, []string{"failure", seconds(e.At), "", string(e.Kind), seconds(e.Detection), seconds(e.Recovery), strconv.Itoa(e.LostSteps), ""})
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
	fmt.Fprintln(w, "fault   count  mean_detect_s  mean_recover_s  mean_lost_steps")
	for _, kind := range []fault.Kind{fault.Crash, fault.Hang, fault.Slow} {
		n := 0
		var d, t time.Duration
		lost := 0
		for _, e := range r.Failures {
			if e.Kind == kind {
				n++
				d += e.Detection
				t += e.Recovery
				lost += e.LostSteps
			}
		}
		if n > 0 {
			fmt.Fprintf(w, "%-7s %5d %14.3f %15.3f %16.2f\n", kind, n, d.Seconds()/float64(n), t.Seconds()/float64(n), float64(lost)/float64(n))
		}
	}
}
