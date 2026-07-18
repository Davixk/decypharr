package config

import "testing"

func TestUsenetReadTimeoutDefaultAndEnvironmentOverride(t *testing.T) {
	cfg := &Config{}
	cfg.updateUsenetConfig()
	if got, want := cfg.Usenet.ReadTimeout, "30s"; got != want {
		t.Fatalf("default read timeout = %q, want %q", got, want)
	}

	t.Setenv("DECYPHARR_USENET__READ_TIMEOUT", "17s")
	cfg.applyUsenetEnvVars()
	if got, want := cfg.Usenet.ReadTimeout, "17s"; got != want {
		t.Fatalf("environment read timeout = %q, want %q", got, want)
	}
}
