//go:build cgo

package yenc

import (
	"errors"

	"github.com/Tensai75/rapidyenc"
)

// ErrNoBinaryData is the ONE decoder verdict that proves an article carries no
// content at all: the decoder consumed the whole article without ever seeing a
// "=ybegin" header. rapidyenc reports it as
//
//	[rapidyenc] end of article without finding "=begin" header: no binary data
//
// wrapping its exported sentinel rapidyenc.ErrDataMissing ("no binary data").
//
// This is deliberately NOT the same class as rapidyenc.ErrDataCorruption
// ("=yend" trailer missing, size mismatch) or ErrCrcMismatch: those mean the
// payload existed but arrived damaged or truncated, which a retry or another
// provider can still resolve. Only the no-payload case is a definitive verdict
// about the content itself.
var ErrNoBinaryData = rapidyenc.ErrDataMissing

// IsNoBinaryData reports whether err is (or wraps) the no-payload verdict.
func IsNoBinaryData(err error) bool {
	return err != nil && errors.Is(err, ErrNoBinaryData)
}
