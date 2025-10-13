package gocan

import "fmt"

// TimeoutError comes from roffe/gocan initial repo
type TimeoutError struct {
	Timeout int64
	Frames  []uint32
	Type    string
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("%s timeout (%dms) for frame 0x%03X", e.Type, e.Timeout, e.Frames)
}
