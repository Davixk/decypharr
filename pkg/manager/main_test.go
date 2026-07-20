package manager

import (
	"os"
	"testing"

	"github.com/sirrobot01/decypharr/internal/utils"
)

// TestMain starts the global cached-time updater for the whole package, exactly
// as production does (cmd/decypharr/main.go). Without it, utils.Now() stays
// frozen at package-init time: every NNTP handshake deadline is computed as
// utils.Now()+HandshakeTimeout, so once the suite has been running longer than
// HandshakeTimeout (10s) that deadline is already in the past and connections
// fail instantly with a spurious "read greeting: i/o timeout". Starting the
// updater keeps utils.Now() tracking the wall clock so handshake deadlines stay
// in the future regardless of how long the suite runs.
func TestMain(m *testing.M) {
	utils.StartGlobalCachedTime()
	code := m.Run()
	utils.StopGlobalCachedTime()
	os.Exit(code)
}
