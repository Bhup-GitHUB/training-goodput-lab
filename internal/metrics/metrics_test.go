package metrics

import (
	"bytes"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"training-goodput-lab/internal/fault"
)

func TestUndetectedFailuresAreCensored(t *testing.T) {
	run := Run{
		Workers: 16, Seed: 42, WallTime: time.Hour, UsefulTime: 30 * time.Minute,
		CheckpointTime: time.Minute, Steps: 60, SparesUsed: 1,
		Failures: []Failure{
			{Kind: fault.Hang, Detection: 2 * time.Second, Recovery: 4 * time.Second, LostSteps: 3, Detected: true},
			{Kind: fault.Hang, Detection: 900 * time.Second},
		},
	}
	path := filepath.Join(t.TempDir(), "metrics.csv")
	if err := WriteCSV(path, run); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	columns := make(map[string]int)
	for index, name := range rows[0] {
		columns[name] = index
	}
	for name, expected := range map[string]string{"workers": "16", "seed": "42", "spares_used": "1", "checkpoint_seconds": "60.000000", "useful_time_seconds": "1800.000000", "wall_time_seconds": "3600.000000", "detected_failures": "1", "undetected_failures": "1"} {
		if rows[1][columns[name]] != expected {
			t.Fatalf("summary %s: got %q, want %q", name, rows[1][columns[name]], expected)
		}
	}
	censored := rows[3]
	if censored[columns["detected"]] != "false" || censored[columns["detection_seconds"]] != "" || censored[columns["censored_elapsed_seconds"]] != "900.000000" {
		t.Fatalf("undetected failure is not explicitly censored: %v", censored)
	}
	var summary bytes.Buffer
	PrintSummary(&summary, run)
	if !strings.Contains(summary.String(), "undetected_censored") || !strings.Contains(summary.String(), "2.000") || strings.Contains(summary.String(), "451.000") {
		t.Fatalf("summary mixes censored observations into detection mean: %s", summary.String())
	}
}

func TestGoodputBounds(t *testing.T) {
	for _, test := range []struct {
		wall, useful time.Duration
		want         float64
	}{
		{time.Second, 2 * time.Second, 1},
		{time.Second, -time.Second, 0},
		{0, time.Second, 0},
		{2 * time.Second, time.Second, 0.5},
	} {
		if got := (Run{WallTime: test.wall, UsefulTime: test.useful}).Goodput(); got != test.want {
			t.Fatalf("Goodput(%s, %s) = %g, want %g", test.wall, test.useful, got, test.want)
		}
	}
}
