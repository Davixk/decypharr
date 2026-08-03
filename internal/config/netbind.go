package config

// NetworkBindingConfig chooses which local network interface outbound traffic
// leaves from, per class of operation.
//
// The case it exists for: send everything over the ordinary route, but send
// TRACKER traffic through a WireGuard tunnel. Those destinations differ in who
// runs them — the debrid and usenet providers are companies the operator pays
// and holds an account with, while a public tracker is run by somebody unknown.
//
// ⚠️ FAIL CLOSED. A configured binding that cannot be resolved makes the
// operation FAIL. It never silently falls back to the default route, because a
// silent fallback sends the traffic exactly where it was configured not to go
// and gives no sign it happened.
//
// ⚠️ THE INTERFACE MUST EXIST IN THIS PROCESS'S NETWORK NAMESPACE. A container
// on a bridge network sees only its own veth and cannot bind a host tunnel,
// however correct the name is — measured on a live deployment, decypharr saw
// only lo and eth0 172.16.11.2/24 while the WireGuard interface lived on the
// host. Binding is a process-level control; putting the interface within reach
// is a deployment-level one, and this config cannot substitute for it. When the
// interface is unreachable the binding fails closed, which is the intended
// outcome: visibly broken beats invisibly untunnelled.
type NetworkBindingConfig struct {
	// Default applies to every outbound operation without a more specific
	// binding. Empty means the OS default route — today's behaviour, so an
	// absent config changes nothing.
	Default string `json:"default,omitempty"`

	// Tracker is BitTorrent tracker traffic: currently the BEP 15 scrape used
	// by the seeder gate.
	//
	// Worth knowing what a scrape does and does not expose. A BEP 15 scrape
	// (action=2) carries connection_id, action, transaction_id and infohashes —
	// no port, no peer_id, no event — so a tracker cannot enter this host into
	// the swarm from one; it has nothing to advertise. Only announce (action=1)
	// does that. The exposure is therefore the same class as any other API
	// call: that tracker's operator sees a source IP in their own logs, and no
	// peer ever does. Tunnelling it is a reasonable preference about
	// counterparties rather than a fix for a leak.
	Tracker string `json:"tracker,omitempty"`
}

// Bindings returns the configured class -> interface mapping.
//
// Only classes something actually honours are represented. A knob that reads as
// protection and provides none is worse than an absent one, so a new class
// appears here together with the code that consults it.
func (n NetworkBindingConfig) Bindings() map[string]string {
	out := make(map[string]string, 2)
	if n.Default != "" {
		out["default"] = n.Default
	}
	if n.Tracker != "" {
		out["tracker"] = n.Tracker
	}
	return out
}

func (n NetworkBindingConfig) IsZero() bool {
	return n.Default == "" && n.Tracker == ""
}
