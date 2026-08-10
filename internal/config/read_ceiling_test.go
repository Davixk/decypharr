package config

import "testing"

// TestReadCeilingsAreSeededAndHot covers both halves of a ceiling knob's
// contract, and both matter for the same reason.
//
// SEEDED: an existing config.json predates these keys, so it arrives with them
// empty. If setDefaults did not fill them in, every upgraded install would run
// with an unset ceiling — which is exactly the unbounded state they exist to
// remove, silently, on the installs most likely to be hit by it.
//
// HOT: these are the knobs an operator reaches for while a backend is actively
// flapping. If they were classified restart-required, tuning them mid-incident
// would mean taking the mount down to fix the mount.
func TestReadCeilingsAreSeededAndHot(t *testing.T) {
	current := &Config{}
	current.setDefaults()

	if current.DebridLinkTimeout != DefaultDebridLinkTimeout {
		t.Fatalf("debrid_link_timeout default = %q, want %q", current.DebridLinkTimeout, DefaultDebridLinkTimeout)
	}
	if current.MetadataReadTimeout != DefaultMetadataReadTimeout {
		t.Fatalf("metadata_read_timeout default = %q, want %q", current.MetadataReadTimeout, DefaultMetadataReadTimeout)
	}
	if current.DebridStatusTimeout != DefaultDebridStatusTimeout {
		t.Fatalf("debrid_status_timeout default = %q, want %q", current.DebridStatusTimeout, DefaultDebridStatusTimeout)
	}

	for name, mutate := range map[string]func(*Config){
		"debrid_link_timeout":   func(c *Config) { c.DebridLinkTimeout = "5s" },
		"metadata_read_timeout": func(c *Config) { c.MetadataReadTimeout = "3s" },
		"debrid_status_timeout": func(c *Config) { c.DebridStatusTimeout = "30s" },
	} {
		updated := *current
		mutate(&updated)
		if current.RequiresRestart(&updated) {
			t.Fatalf("%s was classified restart-required; it is resolved live per request", name)
		}
	}
}
