package config

import "testing"

func TestFromEnvUsesDefaultServiceAddresses(t *testing.T) {
	t.Setenv("TERMINAL_PROXY_TICKET_SIGNING_KEY", "01234567890123456789012345678901!")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("from env: %v", err)
	}
	if cfg.RunnersAddress != "runners:50051" {
		t.Fatalf("unexpected runners address %q", cfg.RunnersAddress)
	}
	if cfg.AgentsAddress != "agents:50051" {
		t.Fatalf("unexpected agents address %q", cfg.AgentsAddress)
	}
	if cfg.AuthorizationAddress != "authorization:50051" {
		t.Fatalf("unexpected authorization address %q", cfg.AuthorizationAddress)
	}
}

func TestFromEnvUsesCustomAuthorizationAddress(t *testing.T) {
	t.Setenv("TERMINAL_PROXY_TICKET_SIGNING_KEY", "01234567890123456789012345678901!")
	t.Setenv("AUTHORIZATION_ADDRESS", "authorization.internal:7443")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("from env: %v", err)
	}
	if cfg.AuthorizationAddress != "authorization.internal:7443" {
		t.Fatalf("unexpected authorization address %q", cfg.AuthorizationAddress)
	}
}

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
