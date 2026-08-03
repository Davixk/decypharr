// Package netbind chooses which local network interface an outbound operation
// leaves from, per class of operation.
//
// WHY PER CLASS. The operator's case is precise: send everything over the
// ordinary route, but send TRACKER traffic through a WireGuard tunnel. Those
// destinations differ in who runs them — RealDebrid, AllDebrid, Newshosting and
// Proton are companies he pays and has an account with, while a public tracker
// is run by somebody unknown. That is a policy about counterparties, not about
// protocols, so the binding is configured by NAMED CLASS rather than inferred
// from a destination address.
//
// ⚠️ FAIL CLOSED. If a class has a binding configured and it cannot be resolved
// or bound, the operation FAILS. It must never quietly fall back to the default
// route: a silent fallback sends the traffic exactly where the operator
// configured it not to go, and does so invisibly. An error is recoverable; an
// unnoticed leak is not.
//
// Names are preferred over addresses for the same reason. "wgProtonCH" survives
// a tunnel reconnect; "10.2.0.2" is closer to a lease, and a reconnect that
// changed it would otherwise turn into a silent fallback at the worst moment.
// Both are accepted, and a name is resolved to its CURRENT address at connect
// time rather than once at startup.
package netbind

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"
)

// Class names a category of outbound operation.
//
// Classes are explicit rather than derived from the destination. Two calls to
// the same host can belong to different classes, and an operator reading a
// config should be able to see what is bound where without knowing which host
// each subsystem talks to.
type Class string

const (
	// ClassDefault applies to every outbound operation without a more specific
	// binding. Unset means the OS default route, i.e. today's behaviour.
	ClassDefault Class = "default"
	// ClassTracker is BitTorrent tracker traffic — currently the BEP 15 scrape.
	ClassTracker Class = "tracker"
)

// Classes is every class this build can actually honour.
//
// DELIBERATELY SHORT. A class listed here is one something is wired to; a class
// that could be configured but that nothing consults would be a knob that reads
// as protection and provides none — the same failure as a seeder gate with no
// trackers to ask, which looked identical to a working one. New classes land
// with their consumer, not before it.
var Classes = []Class{ClassDefault, ClassTracker}

// ErrUnbindable reports a configured binding that could not be resolved.
type ErrUnbindable struct {
	Class Class
	Spec  string
	Err   error
}

func (e *ErrUnbindable) Error() string {
	return fmt.Sprintf("network binding for %q (%q) could not be resolved: %v. "+
		"Refusing to fall back to the default route, which would send this traffic "+
		"exactly where it was configured not to go", e.Class, e.Spec, e.Err)
}

func (e *ErrUnbindable) Unwrap() error { return e.Err }

// Binder resolves a class to a local address.
type Binder struct {
	specs map[Class]string
}

// New builds a Binder from class -> interface-name-or-IP.
func New(specs map[Class]string) *Binder {
	cleaned := make(map[Class]string, len(specs))
	for class, spec := range specs {
		if spec = strings.TrimSpace(spec); spec != "" {
			cleaned[class] = spec
		}
	}
	return &Binder{specs: cleaned}
}

// Spec returns the configured binding for a class, falling back to the default
// class. The second return reports whether anything is configured at all.
func (b *Binder) Spec(class Class) (string, bool) {
	if b == nil {
		return "", false
	}
	if spec, ok := b.specs[class]; ok {
		return spec, true
	}
	spec, ok := b.specs[ClassDefault]
	return spec, ok
}

// LocalIP resolves the address a class should egress from.
//
// Returns (nil, false, nil) when nothing is configured — the OS picks, which is
// the unconfigured default and not an error. Returns an error only when a
// binding IS configured and cannot be honoured.
func (b *Binder) LocalIP(class Class) (net.IP, bool, error) {
	spec, ok := b.Spec(class)
	if !ok {
		return nil, false, nil
	}
	ip, err := resolveSpec(spec)
	if err != nil {
		return nil, true, &ErrUnbindable{Class: class, Spec: spec, Err: err}
	}
	return ip, true, nil
}

// resolveSpec accepts an IP literal or an interface name.
//
// A name is resolved every time rather than cached: a tunnel that reconnects
// with a new address must keep working, and — more importantly — a tunnel that
// has gone away must start FAILING rather than resolving to a stale address
// that no longer routes.
func resolveSpec(spec string) (net.IP, error) {
	if ip := net.ParseIP(spec); ip != nil {
		return ip, nil
	}
	iface, err := net.InterfaceByName(spec)
	if err != nil {
		return nil, fmt.Errorf("no interface named %q: %w", spec, err)
	}
	if iface.Flags&net.FlagUp == 0 {
		return nil, fmt.Errorf("interface %q is down", spec)
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return nil, fmt.Errorf("interface %q addresses: %w", spec, err)
	}
	var v6 net.IP
	for _, addr := range addrs {
		var ip net.IP
		switch a := addr.(type) {
		case *net.IPNet:
			ip = a.IP
		case *net.IPAddr:
			ip = a.IP
		}
		if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			continue
		}
		if v4 := ip.To4(); v4 != nil {
			return v4, nil
		}
		if v6 == nil {
			v6 = ip
		}
	}
	if v6 != nil {
		return v6, nil
	}
	return nil, fmt.Errorf("interface %q has no usable address", spec)
}

// UDPAddr returns the local address for a UDP socket, or nil when unconfigured.
func (b *Binder) UDPAddr(class Class) (*net.UDPAddr, error) {
	ip, configured, err := b.LocalIP(class)
	if err != nil || !configured {
		return nil, err
	}
	return &net.UDPAddr{IP: ip}, nil
}

// DialContext returns a dial function bound to a class.
//
// The binding is resolved PER DIAL, so a reconnected tunnel is picked up and a
// vanished one produces an error instead of a silent fallback.
func (b *Binder) DialContext(class Class, timeout, keepAlive time.Duration) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := &net.Dialer{Timeout: timeout, KeepAlive: keepAlive}
		ip, configured, err := b.LocalIP(class)
		if err != nil {
			return nil, err
		}
		if configured {
			if strings.HasPrefix(network, "udp") {
				dialer.LocalAddr = &net.UDPAddr{IP: ip}
			} else {
				dialer.LocalAddr = &net.TCPAddr{IP: ip}
			}
		}
		return dialer.DialContext(ctx, network, addr)
	}
}

// Resolved is one class's binding as it stands right now.
type Resolved struct {
	Class      Class
	Spec       string
	Address    string
	Configured bool
	Err        error
}

// Snapshot resolves every class, for the startup log.
//
// An operator must be able to read from the logs which class egresses where,
// rather than inferring it from the absence of a complaint. A binding that is
// configured but broken is reported here as an error at startup instead of
// surfacing later as a failed grab nobody connects to the config.
func (b *Binder) Snapshot() []Resolved {
	out := make([]Resolved, 0, len(Classes))
	for _, class := range Classes {
		spec, configured := b.Spec(class)
		entry := Resolved{Class: class, Spec: spec, Configured: configured}
		if !configured {
			entry.Address = "(OS default route)"
			out = append(out, entry)
			continue
		}
		ip, _, err := b.LocalIP(class)
		if err != nil {
			entry.Err = err
		} else if ip != nil {
			entry.Address = ip.String()
		}
		out = append(out, entry)
	}
	return out
}
