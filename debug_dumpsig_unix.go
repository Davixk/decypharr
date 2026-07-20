//go:build unix

package main

import (
	"os"
	"syscall"
)

// debugDumpSignals returns the signal(s) that trigger an on-demand, non-fatal
// goroutine dump. On Unix this is SIGUSR1 (user-defined signal 1): the Go
// runtime assigns it no other meaning, and once it is signal.Notify()'d it can
// never terminate the process.
func debugDumpSignals() []os.Signal {
	return []os.Signal{syscall.SIGUSR1}
}
