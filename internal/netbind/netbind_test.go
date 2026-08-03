package netbind

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// The property that matters most here is FAIL CLOSED. A binding exists so that
// certain traffic takes a specific route; falling back to the default route
// when it cannot be honoured sends that traffic exactly where the operator
// configured it not to go, and gives no sign it happened. An error is
// recoverable, an unnoticed leak is not — so most of these assert that a broken
// binding FAILS rather than degrades.

func TestUnconfiguredIsNotAnError(t *testing.T) {
	b := New(nil)
	ip, configured, err := b.LocalIP(ClassTracker)
	if err != nil {
		t.Fatalf("err = %v; an absent binding is the default, not a failure", err)
	}
	if configured || ip != nil {
		t.Fatalf("ip=%v configured=%v; nothing was configured", ip, configured)
	}
}

func TestUnresolvableBindingFailsClosed(t *testing.T) {
	for _, spec := range []string{"wgProtonCH", "no-such-iface0", "999.999.999.999"} {
		b := New(map[Class]string{ClassTracker: spec})
		ip, configured, err := b.LocalIP(ClassTracker)
		if err == nil {
			t.Fatalf("spec %q resolved to %v with no error; an unresolvable binding must fail", spec, ip)
		}
		if !configured {
			t.Fatalf("spec %q reported unconfigured", spec)
		}
		var unbindable *ErrUnbindable
		if !errors.As(err, &unbindable) {
			t.Fatalf("err = %T, want *ErrUnbindable so callers can distinguish it", err)
		}
	}
}

// TestDialFailsRatherThanUsingTheDefaultRoute is the same property at the point
// it actually matters — the dial itself.
func TestDialFailsRatherThanUsingTheDefaultRoute(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	b := New(map[Class]string{ClassDefault: "wgThatDoesNotExist"})
	dial := b.DialContext(ClassDefault, time.Second, time.Second)

	conn, err := dial(context.Background(), "tcp", ln.Addr().String())
	if err == nil {
		conn.Close()
		t.Fatal("dial succeeded on the default route despite a configured binding that cannot be honoured")
	}
}

func TestIPLiteralResolves(t *testing.T) {
	b := New(map[Class]string{ClassTracker: "127.0.0.1"})
	ip, configured, err := b.LocalIP(ClassTracker)
	if err != nil || !configured {
		t.Fatalf("ip=%v configured=%v err=%v", ip, configured, err)
	}
	if !ip.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("ip = %v, want 127.0.0.1", ip)
	}
}

// TestInterfaceNameResolves uses whatever loopback is called on this host, so
// the name path is exercised without assuming an interface that may not exist.
func TestInterfaceNameResolves(t *testing.T) {
	ifaces, err := net.Interfaces()
	if err != nil {
		t.Skipf("cannot enumerate interfaces: %v", err)
	}
	var named string
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil || len(addrs) == 0 {
			continue
		}
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok &&
				!ipnet.IP.IsLoopback() && !ipnet.IP.IsLinkLocalUnicast() {
				named = iface.Name
				break
			}
		}
		if named != "" {
			break
		}
	}
	if named == "" {
		t.Skip("no usable non-loopback interface on this host")
	}

	b := New(map[Class]string{ClassTracker: named})
	ip, configured, err := b.LocalIP(ClassTracker)
	if err != nil || !configured || ip == nil {
		t.Fatalf("interface %q: ip=%v configured=%v err=%v", named, ip, configured, err)
	}
}

// TestClassFallsBackToDefault: the operator's case is "everything on the
// ordinary route, trackers on the tunnel", which means an unset class inherits
// default rather than becoming unbound.
func TestClassFallsBackToDefault(t *testing.T) {
	b := New(map[Class]string{ClassDefault: "127.0.0.1"})
	spec, ok := b.Spec(ClassTracker)
	if !ok || spec != "127.0.0.1" {
		t.Fatalf("spec=%q ok=%v; an unset class must inherit the default binding", spec, ok)
	}

	// And a class-specific binding overrides it.
	b = New(map[Class]string{ClassDefault: "127.0.0.1", ClassTracker: "127.0.0.2"})
	if spec, _ := b.Spec(ClassTracker); spec != "127.0.0.2" {
		t.Fatalf("spec = %q, want the tracker-specific binding", spec)
	}
	if spec, _ := b.Spec(ClassDefault); spec != "127.0.0.1" {
		t.Fatalf("spec = %q, want the default binding", spec)
	}
}

func TestBlankSpecsAreIgnored(t *testing.T) {
	b := New(map[Class]string{ClassDefault: "   ", ClassTracker: ""})
	if _, ok := b.Spec(ClassTracker); ok {
		t.Fatal("a whitespace-only binding must count as unset, not as an unresolvable one")
	}
}

// TestSnapshotReportsEveryClass backs the startup log. An operator has to be
// able to READ where each class egresses rather than infer it from silence.
func TestSnapshotReportsEveryClass(t *testing.T) {
	b := New(map[Class]string{ClassTracker: "wgMissing"})
	snapshot := b.Snapshot()
	if len(snapshot) != len(Classes) {
		t.Fatalf("snapshot covers %d classes, want %d", len(snapshot), len(Classes))
	}

	byClass := map[Class]Resolved{}
	for _, r := range snapshot {
		byClass[r.Class] = r
	}
	if got := byClass[ClassDefault]; got.Configured {
		t.Fatalf("default = %+v, want unconfigured", got)
	} else if got.Address == "" {
		t.Fatal("an unconfigured class must still say where it will go")
	}
	if got := byClass[ClassTracker]; got.Err == nil {
		t.Fatalf("tracker = %+v; a broken binding must surface at startup, not hours later", got)
	}
}

// TestUDPAddrIsNilWhenUnconfigured keeps "no binding" distinct from "bind to
// the zero address", which would be a very different instruction to the OS.
func TestUDPAddrIsNilWhenUnconfigured(t *testing.T) {
	addr, err := New(nil).UDPAddr(ClassTracker)
	if err != nil || addr != nil {
		t.Fatalf("addr=%v err=%v; unconfigured must yield no local address at all", addr, err)
	}
}
