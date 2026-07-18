package config

import "testing"

func TestFromEnvUsesMountedZitiIdentityFile(t *testing.T) {
	t.Setenv("TERMINAL_PROXY_TICKET_SIGNING_KEY", "01234567890123456789012345678901!")
	t.Setenv("ZITI_ENABLED", "true")
	t.Setenv("ZITI_IDENTITY_FILE", "/var/run/agyn/terminal-proxy/identity.json")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("from env: %v", err)
	}
	if !cfg.ZitiEnabled {
		t.Fatal("expected ziti enabled")
	}
	if cfg.ZitiIdentityFile != "/var/run/agyn/terminal-proxy/identity.json" {
		t.Fatalf("unexpected ziti identity file %q", cfg.ZitiIdentityFile)
	}
}
