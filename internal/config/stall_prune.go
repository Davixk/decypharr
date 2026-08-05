package config

// StallPruneConfig governs deleting torrents that will not finish, releasing
// the provider slots they hold.
//
// THERE IS ONE TEST, AND IT IS AN ETA.
//
//	age >= MaxDownloadingTime  -> prune (failsafe, regardless of anything else)
//	age <  ETASampleWindow     -> NO VERDICT: there is not yet enough data
//	otherwise                  -> ETA = remaining / speed-over-the-window
//	ETA > MaxETA               -> prune
//
// A dead transfer prunes because its ETA is infinite. A trickling one prunes
// because 40 KB/s against 8 GB is 55 hours. Both fall out of the same test;
// neither needs a stall detector, and earlier versions of this file had two.
//
// THE TEST RECURS. Every sweep re-judges every in-flight transfer on fresh
// data, so nothing survives on one lucky reading and nothing dies on one
// unlucky one.
type StallPruneConfig struct {
	// ETASampleWindow is how long a transfer must have been running before its
	// ETA means anything — and, identically, the window the speed is averaged
	// over.
	//
	// ONE KNOB FOR BOTH ROLES BECAUSE THEY ARE THE SAME THING. Torrent speeds
	// and peer counts float constantly, so a reading taken over a few seconds
	// is noise. This window is what makes the number trustworthy, which is also
	// exactly what makes a momentary lull harmless: a transfer that pauses for
	// one sweep is averaged against the rest of the window rather than reading
	// as stopped.
	//
	// Empty disables the whole feature.
	ETASampleWindow string `json:"eta_sample_window,omitempty"`

	// MaxETA is the ceiling on projected completion, and the only judgement
	// this feature makes. Empty disables it.
	MaxETA string `json:"max_eta,omitempty"`

	// MaxDownloadingTime is a hard failsafe: nothing may sit in a downloading
	// state longer than this, whatever its ETA, speed or progress say.
	//
	// It is the backstop for whatever the ETA test gets wrong, and it is the
	// one rule that needs no measurement at all — the provider's own `added`
	// timestamp is enough, so it still applies after a restart when no speed
	// samples exist yet.
	//
	// MUST BE >= ETASampleWindow + MaxETA. Anything lower would delete
	// transfers that are still inside the ETA they were explicitly allowed,
	// which makes the failsafe contradict the test it exists to back up.
	MaxDownloadingTime string `json:"max_downloading_time,omitempty"`

	// MaxPerSweep caps deletions per pass so a misconfigured threshold drains
	// visibly instead of emptying an account in one tick. Defaults to 25.
	MaxPerSweep int `json:"max_per_sweep,omitempty"`
}

// DefaultStallPruneMaxPerSweep bounds the blast radius of a bad threshold.
const DefaultStallPruneMaxPerSweep = 25

func (s StallPruneConfig) IsZero() bool {
	return s.ETASampleWindow == "" && s.MaxETA == "" &&
		s.MaxDownloadingTime == "" && s.MaxPerSweep == 0
}
