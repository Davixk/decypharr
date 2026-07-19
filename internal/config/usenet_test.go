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

func TestUsenetDownloadTimeoutDefaultAndEnvironmentOverride(t *testing.T) {
	cfg := &Config{}
	cfg.updateUsenetConfig()
	if got, want := cfg.Usenet.DownloadTimeout, "60s"; got != want {
		t.Fatalf("default download timeout = %q, want %q", got, want)
	}

	t.Setenv("DECYPHARR_USENET__DOWNLOAD_TIMEOUT", "90s")
	cfg.applyUsenetEnvVars()
	if got, want := cfg.Usenet.DownloadTimeout, "90s"; got != want {
		t.Fatalf("environment download timeout = %q, want %q", got, want)
	}
}

func TestUsenetTimeoutDisableSpellingsSurviveDefaulting(t *testing.T) {
	// Explicit disable values must not be replaced by the defaults.
	cfg := &Config{}
	cfg.Usenet.ReadTimeout = "off"
	cfg.Usenet.DownloadTimeout = "0"
	cfg.updateUsenetConfig()
	if got, want := cfg.Usenet.ReadTimeout, "off"; got != want {
		t.Fatalf("read timeout = %q, want %q", got, want)
	}
	if got, want := cfg.Usenet.DownloadTimeout, "0"; got != want {
		t.Fatalf("download timeout = %q, want %q", got, want)
	}
}
