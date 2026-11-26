package nntp

import (
	"os"
	"testing"

	"github.com/sirrobot01/decypharr/internal/utils"
)

// TestMain starts the global cached-time updater for the whole package, exactly
// as production does (cmd/decypharr/main.go). Without it, utils.Now() stays
// frozen at package-init time: Connection.ping sets its deadline to
// utils.Now()+PingTimeout (1.5s), so once the suite has been running longer
// than that the deadline is already in the past and every keepalive ping fails
// instantly. The reaper tests then pass or fail depending on how long the tests
// ahead of them took. Starting the updater keeps utils.Now() within one tick
// (500ms) of the wall clock, regardless of test order or suite length.
func TestMain(m *testing.M) {
	utils.StartGlobalCachedTime()
	code := m.Run()
	utils.StopGlobalCachedTime()
	os.Exit(code)
}
