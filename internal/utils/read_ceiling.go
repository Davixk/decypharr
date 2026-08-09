package utils

import (
	"fmt"
	"strings"
	"time"
)

// ParseReadCeiling resolves one of decypharr's read-ceiling knobs
// (debrid_read_timeout, debrid_link_timeout, metadata_read_timeout) with the
// SAME semantics for all of them, so an operator who has learned one has
// learned the others:
//
//	""                -> fallback (the knob is simply unset)
//	"off" / "none"    -> 0, meaning DISABLED: no ceiling at all
//	"0", "0s", ...    -> 0, same as above
//	"90s", "2m", "1h" -> that duration
//	negative/garbage  -> fallback, plus an error the caller is expected to LOG
//
// The error is returned rather than swallowed on purpose: a typo in a ceiling
// silently reverting to "unbounded" is the exact failure these knobs exist to
// prevent, so every caller must be able to say so out loud.
//
// It lives in utils rather than config because internal/config cannot import
// anything that reaches back into it (logger -> config is already an edge), and
// every consumer of a ceiling is outside config anyway.
func ParseReadCeiling(raw string, fallback time.Duration) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return fallback, nil
	}
	switch strings.ToLower(trimmed) {
	case "off", "none":
		return 0, nil
	}
	d, err := ParseDuration(trimmed)
	if err != nil {
		return fallback, err
	}
	if d < 0 {
		return fallback, fmt.Errorf("negative duration %q", raw)
	}
	return d, nil
}
