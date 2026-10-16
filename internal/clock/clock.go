package clock

import "time"

type Virtual struct{ now time.Duration }

func (v *Virtual) Now() time.Duration { return v.now }
func (v *Virtual) Advance(d time.Duration) {
	if d < 0 {
		panic("clock cannot go backwards")
	}
	v.now += d
}
