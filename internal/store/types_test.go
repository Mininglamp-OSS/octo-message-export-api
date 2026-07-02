package store

import "testing"

func TestStatusString(t *testing.T) {
	cases := map[Status]string{
		StatusQueued:    "queued",
		StatusRunning:   "running",
		StatusCompleted: "completed",
		StatusPartial:   "partial",
		StatusFailed:    "failed",
		StatusCancelled: "cancelled",
		Status(99):      "unknown",
	}
	for st, want := range cases {
		if got := st.String(); got != want {
			t.Errorf("Status(%d).String() = %q, want %q", st, got, want)
		}
	}
}

func TestStatusIsTerminal(t *testing.T) {
	terminal := []Status{StatusCompleted, StatusPartial, StatusFailed, StatusCancelled}
	nonTerminal := []Status{StatusQueued, StatusRunning}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("%s should not be terminal", s)
		}
	}
}
