package fault

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

type Kind string

const (
	Crash Kind = "crash"
	Hang  Kind = "hang"
	Slow  Kind = "slow"
)

type Mix struct{ Crash, Hang, Slow float64 }

func DefaultMix() Mix           { return Mix{Crash: .7, Hang: .15, Slow: .15} }
func Llama3MTBF() time.Duration { return time.Duration(float64(54*24*time.Hour) * 16384 / 419) }
func (m Mix) Validate() error {
	if math.IsNaN(m.Crash+m.Hang+m.Slow) || math.IsInf(m.Crash+m.Hang+m.Slow, 0) || m.Crash < 0 || m.Hang < 0 || m.Slow < 0 || m.Crash+m.Hang+m.Slow <= 0 {
		return fmt.Errorf("fault weights must be nonnegative with positive sum")
	}
	return nil
}
func (m Mix) Sample(r *rand.Rand) Kind {
	x := r.Float64() * (m.Crash + m.Hang + m.Slow)
	if x < m.Crash {
		return Crash
	}
	if x < m.Crash+m.Hang {
		return Hang
	}
	return Slow
}
func Next(r *rand.Rand, mean time.Duration) time.Duration {
	return time.Duration(r.ExpFloat64() * float64(mean))
}
