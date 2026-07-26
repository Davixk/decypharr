package nntp

import (
	"errors"
	"fmt"
	"testing"

	"github.com/sirrobot01/decypharr/internal/nntp/yenc"
)

// payloadMissingError reproduces the exact error chain a stubbed article
// produces in production:
//
//	NNTP YENC_DECODE (code 0): yEnc decode failed - streaming yenc decode
//	failed: [rapidyenc] end of article without finding "=begin" header:
//	no binary data
//
// classifyTransferError builds the outer *Error; the rapidyenc sentinel is
// wrapped with %w all the way down, so errors.Is finds it.
func payloadMissingError() error {
	return classifyTransferError("streaming yenc decode failed",
		fmt.Errorf("[rapidyenc] end of article without finding %q header: %w", "=begin", yenc.ErrNoBinaryData))
}

func TestIsArticlePayloadMissingErrorOnlyMatchesTheNoPayloadVerdict(t *testing.T) {
	corrupt := classifyTransferError("streaming yenc decode failed",
		errors.New(`[rapidyenc] end of article without finding "=yend" trailer: data corruption`))
	sizeMismatch := classifyTransferError("streaming yenc decode failed",
		errors.New("[rapidyenc] expected size 750000 but got 12: data corruption"))

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"no binary data", payloadMissingError(), true},
		{"wrapped no binary data", fmt.Errorf("read segment 3: %w", payloadMissingError()), true},
		{"truncated body", corrupt, false},
		{"size mismatch", sizeMismatch, false},
		{"430 article not found", &Error{Type: ErrorTypeArticleNotFound, Code: 430}, false},
		{"connection", NewConnectionError(errors.New("reset")), false},
		{"bare sentinel without nntp classification", yenc.ErrNoBinaryData, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsArticlePayloadMissingError(tc.err); got != tc.want {
				t.Fatalf("IsArticlePayloadMissingError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestIsContentMissingErrorCoversBothDefinitiveVerdicts(t *testing.T) {
	if !IsContentMissingError(&Error{Type: ErrorTypeArticleNotFound, Code: 430}) {
		t.Fatal("430 must classify as content-missing")
	}
	if !IsContentMissingError(payloadMissingError()) {
		t.Fatal("no-payload article must classify as content-missing")
	}
	// A truncated body may succeed on retry or on another provider; treating it
	// as dead content would durably delete recoverable files.
	corrupt := classifyTransferError("streaming yenc decode failed",
		errors.New(`[rapidyenc] end of article without finding "=yend" trailer: data corruption`))
	if IsContentMissingError(corrupt) {
		t.Fatal("a truncated/corrupt body must NOT classify as content-missing")
	}
	if IsContentMissingError(NewTimeoutError(errors.New("i/o timeout"))) {
		t.Fatal("a timeout must NOT classify as content-missing")
	}
	// The no-payload verdict is a content verdict, never an infrastructure one.
	if IsInfrastructureError(payloadMissingError()) {
		t.Fatal("no-payload article must not classify as infrastructure")
	}
}
