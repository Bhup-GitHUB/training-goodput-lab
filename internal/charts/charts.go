package charts

import (
	"fmt"
	"image/color"
	"os"
	"path/filepath"
	"strings"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
	"training-goodput-lab/internal/metrics"
)

type XY struct{ X, Y float64 }
type Series struct {
	Name   string
	Points []XY
}

func palette(index int) color.Color {
	colors := []color.RGBA{{31, 104, 177, 255}, {224, 117, 39, 255}, {43, 143, 106, 255}, {183, 62, 75, 255}, {122, 86, 164, 255}}
	return colors[index%len(colors)]
}

func RenderLines(path, title, xLabel, yLabel string, series []Series, optimum float64) error {
	p := plot.New()
	p.Title.Text = title
	p.X.Label.Text = xLabel
	if strings.Contains(xLabel, "Checkpoint interval") || strings.Contains(xLabel, "Active GPUs") {
		p.X.Scale = plot.LogScale{}
		p.X.Tick.Marker = plot.LogTicks{}
	}
	p.Y.Label.Text = yLabel
	p.Add(plotter.NewGrid())
	p.Legend.Top = true
	for i, s := range series {
		if len(s.Points) == 0 {
			continue
		}
		points := make(plotter.XYs, len(s.Points))
		for j, point := range s.Points {
			points[j].X = point.X
			points[j].Y = point.Y
		}
		line, marks, err := plotter.NewLinePoints(points)
		if err != nil {
			return err
		}
		line.Color = palette(i)
		line.Width = vg.Points(2)
		marks.Color = palette(i)
		marks.Radius = vg.Points(3)
		p.Add(line, marks)
		p.Legend.Add(s.Name, line, marks)
	}
	if strings.Contains(strings.ToLower(yLabel), "goodput") {
		p.Y.Min = 0
		p.Y.Max = 1
	}
	if optimum > 0 {
		marker, err := plotter.NewLine(plotter.XYs{{X: optimum, Y: 0}, {X: optimum, Y: 1}})
		if err != nil {
			return err
		}
		marker.Color = palette(3)
		marker.Dashes = []vg.Length{vg.Points(5), vg.Points(4)}
		p.Add(marker)
		p.Legend.Add(fmt.Sprintf("Young approximation: %.1f steps", optimum), marker)
	}
	return save(p, path)
}

func RenderDetection(path string, watchdog, heartbeat float64, censored bool) error {
	p := plot.New()
	p.Title.Text = "Silent hangs: detection versus observation limit"
	p.Y.Label.Text = "Mean seconds since hang (log scale)"
	for i, seconds := range []float64{watchdog, heartbeat} {
		value := max(1, seconds)
		stem, err := plotter.NewLine(plotter.XYs{{X: float64(i), Y: 1}, {X: float64(i), Y: value}})
		if err != nil {
			return err
		}
		stem.Color = palette(i)
		stem.Width = vg.Points(4)
		point, err := plotter.NewScatter(plotter.XYs{{X: float64(i), Y: value}})
		if err != nil {
			return err
		}
		point.Color = palette(i)
		point.Radius = vg.Points(6)
		if i == 1 && censored {
			point.Shape = draw.TriangleGlyph{}
		}
		p.Add(stem, point)
	}
	p.X.Min = -0.5
	p.X.Max = 1.5
	second := "Heartbeats only"
	if censored {
		second += " (undetected; lower bound)"
	}
	p.NominalX("Heartbeats + progress watchdog", second)
	p.Y.Scale = plot.LogScale{}
	p.Y.Tick.Marker = plot.LogTicks{}
	p.Y.Min = 1
	return save(p, path)
}

func RenderTimeline(path string, points []metrics.Point) error {
	p := plot.New()
	p.Title.Text = "Training progress, failures and rollback"
	p.X.Label.Text = "Elapsed minutes"
	p.Y.Label.Text = "Training step"
	p.Y.Tick.Marker = plot.TickerFunc(func(low, high float64) []plot.Tick {
		ticks := (plot.DefaultTicks{}).Ticks(low, high)
		for i := range ticks {
			if ticks[i].Label != "" {
				ticks[i].Label = fmt.Sprintf("%.0f", ticks[i].Value)
			}
		}
		return ticks
	})
	p.Add(plotter.NewGrid())
	p.Legend.Top = true
	p.Legend.Left = true
	if len(points) == 0 {
		return fmt.Errorf("timeline has no points")
	}
	progress := make(plotter.XYs, len(points))
	events := map[string]plotter.XYs{}
	for i, point := range points {
		xy := plotter.XY{X: point.Time.Minutes(), Y: float64(point.Step)}
		progress[i] = xy
		for _, event := range []string{"failure", "rollback", "recover", "checkpoint"} {
			if strings.Contains(point.Event, event) {
				events[event] = append(events[event], xy)
				break
			}
		}
	}
	line, err := plotter.NewLine(progress)
	if err != nil {
		return err
	}
	line.Color = palette(0)
	p.Add(line)
	p.Legend.Add("Step", line)
	for i, event := range []string{"failure", "rollback", "recover", "checkpoint"} {
		if len(events[event]) == 0 {
			continue
		}
		marks, err := plotter.NewScatter(events[event])
		if err != nil {
			return err
		}
		marks.Color = palette(i + 1)
		marks.Radius = vg.Points(4)
		if event == "failure" {
			marks.Shape = draw.CrossGlyph{}
		}
		p.Add(marks)
		p.Legend.Add(event, marks)
	}
	return save(p, path)
}

func save(p *plot.Plot, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	if err := p.Save(11*vg.Inch, 6*vg.Inch, path); err != nil {
		return fmt.Errorf("save chart: %w", err)
	}
	return nil
}
