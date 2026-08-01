package alldebrid

import (
	"errors"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// AllDebrid publishes no endpoint reporting remaining capacity, so its ERRORS
// are the capacity signal. These tests pin the two limit codes to distinct
// typed errors, because they clear on completely different timescales and a
// shared retry cadence would spin against a provider that has already refused.

// TestMagnetTooManyActiveIsSlotExhaustion pins the DOCUMENTED concurrency cap.
// AllDebrid documents MAGNET_TOO_MANY_ACTIVE as "Already have maximum allowed
// active magnets (30)" — our own magnets finishing is what frees it.
func TestMagnetTooManyActiveIsSlotExhaustion(t *testing.T) {
	err := newAllDebridAPIError(&errorResponse{
		Code:    "MAGNET_TOO_MANY_ACTIVE",
		Message: "Already have maximum allowed active magnets (30)",
	})

	if !errors.Is(err, customerror.TooManyActiveDownloadsError) {
		t.Fatalf("err = %v, want slot exhaustion: this is the only signal AllDebrid gives that it is full", err)
	}
	if errors.Is(err, customerror.ProviderAddQuotaExhaustedError) {
		t.Fatal("concurrency exhaustion must not classify as an add quota — it clears as our own magnets finish")
	}
	if !strings.Contains(err.Error(), "MAGNET_TOO_MANY_ACTIVE") {
		t.Fatalf("provider text lost: %v", err)
	}
}

// TestMagnetTooManyIsAddQuotaNotSlots is the one that matters operationally.
// MAGNET_TOO_MANY is UNDOCUMENTED by AllDebrid and was observed in production
// firing 6,715 times while the account held ZERO active magnets. Whatever it
// counts, our completions do not release it — so classifying it as slot
// exhaustion would retry it every 30s forever against a provider saying no.
func TestMagnetTooManyIsAddQuotaNotSlots(t *testing.T) {
	err := newAllDebridAPIError(&errorResponse{
		Code:    "MAGNET_TOO_MANY",
		Message: "Magnets limit reached (1000 accross all tabs)", // AllDebrid's own typo
	})

	if !errors.Is(err, customerror.ProviderAddQuotaExhaustedError) {
		t.Fatalf("err = %v, want add-quota exhaustion", err)
	}
	if errors.Is(err, customerror.TooManyActiveDownloadsError) {
		t.Fatal("MAGNET_TOO_MANY must NOT classify as slot exhaustion. It fires with zero active magnets, " +
			"so nothing we finish frees it, and the slot cadence would spin.")
	}
	if !strings.Contains(err.Error(), "1000 accross all tabs") {
		t.Fatalf("provider message lost — the number is the only thing distinguishing this limit: %v", err)
	}
}

// Control against over-classification: an unrelated error must stay untyped and
// reach the caller as an ordinary failure. If this fails, some non-capacity
// condition is being requeued forever instead of surfacing.
func TestNonLimitErrorsAreNotClassifiedAsCapacity(t *testing.T) {
	for _, tc := range []*errorResponse{
		{Code: "MAGNET_INVALID_URI", Message: "Magnet is not valid."},
		{Code: "MAGNET_MUST_BE_PREMIUM", Message: "You must be premium to use this feature."},
		{Code: "AUTH_BAD_APIKEY", Message: "The auth apikey is invalid"},
	} {
		err := newAllDebridAPIError(tc)
		if errors.Is(err, customerror.TooManyActiveDownloadsError) || errors.Is(err, customerror.ProviderAddQuotaExhaustedError) {
			t.Fatalf("%s was classified as a capacity condition: %v", tc.Code, err)
		}
		if !strings.Contains(err.Error(), tc.Code) {
			t.Fatalf("%s: code lost from message: %v", tc.Code, err)
		}
	}
}

// AllDebrid must report that it cannot report, rather than returning a number.
// It previously answered a hardcoded 100 while its own comment admitted the
// provider exposes nothing — 3.3x its documented 30-magnet cap, in the
// permissive direction, handed to any caller as though it were measured.
func TestGetAvailableSlotsReportsUnknownRatherThanGuessing(t *testing.T) {
	ad := &AllDebrid{}
	slots, err := ad.GetAvailableSlots()

	if !errors.Is(err, types.ErrAvailableSlotsUnknown) {
		t.Fatalf("err = %v, want ErrAvailableSlotsUnknown: a fabricated number is not a safer default than "+
			"an honest absence", err)
	}
	if slots != 0 {
		t.Fatalf("slots = %d, want 0 alongside the unknown sentinel", slots)
	}
}
