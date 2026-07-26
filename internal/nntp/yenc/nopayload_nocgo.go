//go:build !cgo

package yenc

import "errors"

// ErrNoBinaryData mirrors rapidyenc.ErrDataMissing for builds without CGO.
//
// The pure-Go decoder does not currently raise it: an article with no
// "=ybegin" header simply decodes to zero bytes, which the reader's
// validateSegmentLength check already converts into the article-not-found
// verdict. The sentinel is still declared here so callers (and their tests)
// compile and behave identically in both build modes.
var ErrNoBinaryData = errors.New("no binary data")

// IsNoBinaryData reports whether err is (or wraps) the no-payload verdict.
func IsNoBinaryData(err error) bool {
	return err != nil && errors.Is(err, ErrNoBinaryData)
}
