//go:build !unix

package main

import "os"

// debugDumpSignals: no user-defined dump signal exists on this platform
// (e.g. Windows, plan9, js/wasm), so the on-demand goroutine dump is
// unavailable and startDebugDumpHandler becomes a no-op. This keeps the build
// cross-platform.
func debugDumpSignals() []os.Signal { return nil }
