package utils

import (
	"testing"
	"time"
)

// TestParseReadCeiling pins the shared semantics of every read-ceiling knob.
// The two rows that matter most are the last ones: a NEGATIVE or GARBAGE value
// must fall back to the caller's default AND report an error, because a typo
// that silently restored an unbounded wait would defeat the entire point of
// having a ceiling — and would do it invisibly.
func TestParseReadCeiling(t *testing.T) {
	const fallback = 20 * time.Second

	for _, tc := range []struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{raw: "", want: fallback},
		{raw: "   ", want: fallback},
		{raw: "off", want: 0},
		{raw: "None", want: 0},
		{raw: "0", want: 0},
		{raw: "0s", want: 0},
		{raw: "90s", want: 90 * time.Second},
		{raw: "2m", want: 2 * time.Minute},
		{raw: "-5s", want: fallback, wantErr: true},
		{raw: "banana", want: fallback, wantErr: true},
	} {
		got, err := ParseReadCeiling(tc.raw, fallback)
		if (err != nil) != tc.wantErr {
			t.Fatalf("ParseReadCeiling(%q) err = %v, wantErr %v", tc.raw, err, tc.wantErr)
		}
		if got != tc.want {
			t.Fatalf("ParseReadCeiling(%q) = %s, want %s", tc.raw, got, tc.want)
		}
	}
}
