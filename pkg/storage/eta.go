package storage

import "time"

// EtaUnknown is qBittorrent's sentinel for "unknown or not applicable" — 100
// days in seconds. Clients render it as ∞ rather than a duration.
//
// This matters more than it looks. decypharr previously reported eta: 0, and 0
// in the qBittorrent contract does NOT mean "we don't know" — it means
// "arriving now". Every consumer therefore read a confident wrong value instead
// of an absent one, which is the same defect class as a stalled entry
// advertising "Downloading, 0 B, 0%". An absent answer is safe; a fabricated
// one is not.
const EtaUnknown int64 = 8640000

// AverageSpeed returns bytes/second averaged over the entry's whole lifetime,
// or 0 when it cannot be computed.
//
// Deliberately derived from (bytes transferred / elapsed) rather than from a
// sampled speed history: the numbers are already stored, so there is no buffer
// to keep, nothing to drift, and no window to tune. It is also the quantity the
// operator's stall-pruning design actually specified — "track average download
// speed until X minutes have elapsed" — so the pruning stages and the UI read
// the same value rather than two that disagree.
//
// Note this is the average including any time spent stalled, which is the
// point: a torrent that moved fast for a minute and then stopped for an hour
// has a low average, and that is the honest summary of whether it is going to
// finish.
func (e *Entry) AverageSpeed() int64 {
	if e == nil || e.AddedOn.IsZero() {
		return 0
	}
	elapsed := time.Since(e.AddedOn).Seconds()
	if elapsed <= 0 {
		return 0
	}
	downloaded := e.DownloadedBytes()
	if downloaded <= 0 {
		return 0
	}
	return int64(float64(downloaded) / elapsed)
}

// DownloadedBytes is the number of bytes transferred so far.
//
// Progress is a FRACTION in 0..1 despite an old comment on the field claiming
// 0-100; the qBittorrent shim has always multiplied by it directly, and live
// responses carry values like 0.034.
func (e *Entry) DownloadedBytes() int64 {
	if e == nil || e.Size <= 0 {
		return 0
	}
	return int64(float64(e.Size) * e.Progress)
}

// RemainingBytes is what is left to transfer.
func (e *Entry) RemainingBytes() int64 {
	if e == nil || e.Size <= 0 {
		return 0
	}
	remaining := e.Size - e.DownloadedBytes()
	if remaining < 0 {
		return 0
	}
	return remaining
}

// ETASeconds estimates completion at the CURRENT speed, which is what the
// qBittorrent `eta` field means. Returns EtaUnknown when there is nothing left
// to transfer or no speed to extrapolate from — never 0, which would assert
// imminent completion for a torrent that may be dead.
func (e *Entry) ETASeconds() int64 {
	return etaFrom(e.RemainingBytes(), e.Speed)
}

// ETAAtAverageSpeed estimates completion at the LIFETIME average rather than
// the instantaneous one. This is the figure a stall predicate should use: a
// torrent briefly touching 2 MB/s after an hour at zero has a flattering
// instantaneous ETA and an honest average one.
func (e *Entry) ETAAtAverageSpeed() int64 {
	return etaFrom(e.RemainingBytes(), e.AverageSpeed())
}

func etaFrom(remaining, speed int64) int64 {
	if remaining <= 0 || speed <= 0 {
		return EtaUnknown
	}
	eta := remaining / speed
	if eta <= 0 {
		// Sub-second remainder. Report 1s rather than 0, because 0 is the
		// sentinel-adjacent value that started all of this.
		return 1
	}
	if eta > EtaUnknown {
		return EtaUnknown
	}
	return eta
}
