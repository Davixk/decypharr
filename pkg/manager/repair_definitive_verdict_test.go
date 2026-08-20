package manager

import (
	"context"
	"errors"
	"testing"

	"github.com/sirrobot01/decypharr/internal/customerror"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// 🔴 A FILE THE SERVE PATH 404s ON MUST NOT PROBE `unknown`.
//
// Measured, same entry, same minute:
//
//	GET /webdav/.../The.Bourne.Ultimatum...mkv -> 500 "failed to get download link: 404"
//	POST /api/repair/health/<entry>/check      -> status UNKNOWN, broken_count 0
//
// The probe reached the identical 404 and declined to reach a verdict, because
// the mint returned a bare fmt.Errorf that no typed test could match:
// verifyPayload fell to its indeterminate default and the entry rolled up
// `unknown`. Unknown is non-actionable, so it was never pruned, the arr symlink
// never dangled, nothing reaped it, and Plex opened a file that reads zero
// bytes. 3,017 entries in that bucket against 64 broken in one sweep.
//
// Third time this class has bitten: the cool-off wrapper flattening its error to
// text, the Content-Range check whose verdict nothing read, and a definitive
// status that never became a type.
func TestDefinitiveProviderRefusalProbesBroken(t *testing.T) {
	client := &probeClient{linkErr: customerror.NewContentGoneError(
		errors.New("realdebrid: unrestrict returned HTTP 404 for movie.mkv"))}
	_, r := newProbeFixture(t, client)

	entry := probeTorrentEntry("gonehash", "Gone.Entry")
	res := r.probeTorrentFile(context.Background(), entry, entry.Files["file.mkv"], "file.mkv",
		fileResult{name: "file.mkv"}, RepairRunOptions{UnrestrictLink: true}, true, storage.HealthHealthy)

	if !res.broken {
		t.Fatalf("a definitive provider refusal probed %+v, want broken. The serve path answers 404 for this "+
			"same file with no doubt; a probe that says `unknown` means it is never pruned, the symlink never "+
			"dangles, and a viewer finds out instead", res)
	}
	if res.healthy {
		t.Fatal("a definitive provider refusal probed HEALTHY")
	}
	if rollupStatus([]fileResult{res}) != storage.HealthBroken {
		t.Fatalf("reason %q did not roll up to broken; a non-verdict reason is exactly how 3,017 entries "+
			"became invisible to PRUNE", res.reason)
	}
}

// The same answer arriving through the PAYLOAD read, which is the path the
// reported case actually took: the cheap availability check said "supported",
// the probe went on to read real bytes, and the mint 404'd there.
func TestDefinitivePayloadRefusalIsBrokenNotIndeterminate(t *testing.T) {
	client := &probeClient{linkErr: customerror.NewContentGoneError(
		errors.New("realdebrid: unrestrict returned HTTP 404 for movie.mkv"))}
	_, r := newProbeFixture(t, client)

	entry := probeTorrentEntry("payloadgone", "Payload.Gone")
	res := r.verifyPayload(context.Background(), fileResult{name: "file.mkv"}, "provider",
		func(ctx context.Context) (int64, error) {
			return r.readTorrentPayload(ctx, entry, entry.Files["file.mkv"], "file.mkv", client)
		})

	if !res.broken {
		t.Fatalf("a definitive refusal during the payload read probed %+v, want broken", res)
	}
}

// 🛑 THE CONTROL, AND IT IS THE ONE THAT KEEPS THIS SAFE. A genuinely
// indeterminate answer must STILL be indeterminate. The operator's ruling is
// that a targeted check must always resolve — but resolving an OUTAGE as broken
// is how a provider's bad afternoon deletes a library, which is the failure the
// deletion caps exist for. Only definitive answers may condemn.
func TestOutageAndRateLimitStayIndeterminate(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"hoster outage", customerror.HosterUnavailableError},
		{"rate limited / 5xx", debridTypes.ErrAvailabilityIndeterminate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &probeClient{linkErr: tc.err}
			_, r := newProbeFixture(t, client)

			entry := probeTorrentEntry("flap"+tc.name, "Flapping")
			res := r.probeTorrentFile(context.Background(), entry, entry.Files["file.mkv"], "file.mkv",
				fileResult{name: "file.mkv"}, RepairRunOptions{UnrestrictLink: true}, true, storage.HealthHealthy)

			if res.broken {
				t.Fatalf("%s was condemned as broken. It says nothing about the content, and treating it as a "+
					"verdict is how one bad afternoon at a provider mass-deletes a library", tc.name)
			}
			if res.healthy {
				t.Fatalf("%s probed HEALTHY; an answer we could not get is not evidence the file is fine", tc.name)
			}
		})
	}
}
