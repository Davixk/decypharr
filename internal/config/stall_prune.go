package config

// StallPruneConfig governs deleting torrents that will not finish, releasing
// the provider slots they hold.
//
// Every threshold is a knob because none of them can be chosen for someone
// else: the right numbers depend on a library's own speed and ETA
// distributions, on how many provider slots the account has, and on how much
// the operator would rather wait than lose. A default that deletes data on a
// number we guessed is not a default, it is an accident waiting for its first
// user.
//
// So the whole feature is OFF unless explicitly configured, and each stage is
// off independently. An unparseable value also disables its stage rather than
// falling back — the opposite of how the rest of this config behaves, and
// deliberate: for a destructive setting, "I could not read that" must mean
// "do nothing", never "pick something".
type StallPruneConfig struct {
	// NoProgressAfter enables STAGE 1: delete a torrent that has transferred
	// ZERO bytes for this long. Empty disables the stage.
	//
	// This needs no sampling. Progress is monotonic, so "zero now, added an
	// hour ago" already proves zero bytes across the whole hour.
	//
	// Do not go below ~30m. AllDebrid prunes its own no-peer magnets at 30
	// minutes; anything shorter races the provider to the same delete while
	// risking torrents that are merely slow to start.
	NoProgressAfter string `json:"no_progress_after,omitempty"`

	// MaxETA enables STAGE 2: delete a torrent whose projected completion, at
	// its LIFETIME AVERAGE speed, exceeds this. Empty disables the stage.
	//
	// The average, never the instantaneous rate — a torrent dead for an hour
	// that briefly touches 2 MB/s has a flattering instantaneous ETA and an
	// honest average one, and pruning must not be defeated by a momentary
	// spike. This is why storage.Entry exposes both.
	//
	// Stage 2 is the harder one to threshold and should be set from observed
	// ETA distributions rather than intuition: some genuinely slow torrents do
	// finish, and this stage cannot tell them from the ones that will not.
	MaxETA string `json:"max_eta,omitempty"`

	// MinAge is the grace period before STAGE 2 may act. It exists because a
	// lifetime average is meaningless on a torrent that started seconds ago:
	// a few bytes over a few seconds projects to an absurd ETA, and without
	// this every new torrent would be deleted on arrival.
	//
	// Stage 1 does not need it — its own window is already the grace period.
	// Defaults to 30m when MaxETA is set but this is not.
	MinAge string `json:"min_age,omitempty"`

	// MinSeeders enables STAGE 3: delete a torrent that has moved zero bytes and
	// has fewer than this many seeders, once SeederGrace has elapsed.
	//
	// TRI-STATE, and the distinction is load-bearing:
	//
	//	nil (absent) -> 1. The measured default; see below.
	//	0            -> stage disabled. An explicit "do not judge on seeders".
	//	N            -> require N.
	//
	// A plain int could not express "absent" separately from "explicitly off",
	// and those must not collapse: the default is non-zero, so an operator
	// turning the stage off would otherwise be indistinguishable from one who
	// never mentioned it.
	//
	// WHY 1, MEASURED. Across 107 live RealDebrid transfers sampled against
	// actual outcomes:
	//
	//	seeders  n    stalled (speed 0)
	//	0        14   79%
	//	1-2      49   24%
	//	3+       44   27%
	//
	// The cliff is entirely between 0 and 1. A threshold of 3 buys nothing over
	// 1-2 and would refuse 59% of transfers, so "more is safer" is exactly wrong
	// here.
	//
	// This stage only ever acts inside an already-enabled sweep — it cannot turn
	// stall pruning on by itself (see stallPruneSettings.enabled) — and it only
	// ever acts on torrents that have moved ZERO bytes, which stage 1 would kill
	// on its own window anyway. Its contribution is speed, not reach: it will not
	// touch a torrent that has transferred anything, whatever its seeder count,
	// because a momentary zero reading on a moving torrent must never be fatal.
	//
	// AllDebrid publishes no seeder field, so this is inert there. That costs
	// nothing: AllDebrid runs download_uncached=false, where an uncached release
	// is refused at add time and never reaches a stall.
	MinSeeders *int `json:"min_seeders,omitempty"`

	// SeederGrace is how long a torrent must have existed before STAGE 3 may
	// judge it. RealDebrid reports 0 seeders at 0% progress on a transfer it has
	// only just created, so sampling immediately would condemn every torrent on
	// arrival. Defaults to 10m.
	SeederGrace string `json:"seeder_grace,omitempty"`

	// MaxPerSweep caps deletions per pass so a misconfigured threshold drains
	// visibly instead of emptying an account in one tick. Defaults to 25.
	MaxPerSweep int `json:"max_per_sweep,omitempty"`
}

// DefaultStallPruneMinSeeders is the stage-3 threshold when the sweep is
// enabled and MinSeeders is absent. See the field comment for the measurement.
const DefaultStallPruneMinSeeders = 1

// DefaultStallPruneSeederGrace is the settle window before a seeder count means
// anything. Long enough that a just-created transfer reporting 0 is not read as
// a verdict.
const DefaultStallPruneSeederGrace = "10m"

// DefaultStallPruneMinAge is the stage-2 grace period when MaxETA is configured
// without one. Matches AllDebrid's own 30-minute no-peer rule, which is the
// only provider-published number we have for "long enough to judge".
const DefaultStallPruneMinAge = "30m"

// DefaultStallPruneMaxPerSweep bounds the blast radius of a bad threshold.
const DefaultStallPruneMaxPerSweep = 25

func (s StallPruneConfig) IsZero() bool {
	return s.NoProgressAfter == "" && s.MaxETA == "" && s.MinAge == "" && s.MaxPerSweep == 0 &&
		s.MinSeeders == nil && s.SeederGrace == ""
}
