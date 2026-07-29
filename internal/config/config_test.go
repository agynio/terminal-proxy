package config

import (
	"testing"
	"time"
)

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

func TestFromEnvDefaultsZitiIdentityAcquisition(t *testing.T) {
	t.Setenv("TERMINAL_PROXY_TICKET_SIGNING_KEY", "01234567890123456789012345678901!")
	t.Setenv("ZITI_ENABLED", "true")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("from env: %v", err)
	}
	if !cfg.ZitiEnabled {
		t.Fatal("expected ziti enabled")
	}
	if cfg.ZitiManagementAddress != "ziti-management:50051" {
		t.Fatalf("unexpected ziti management address %q", cfg.ZitiManagementAddress)
	}
	if cfg.ZitiEnrollmentTimeout != 2*time.Minute {
		t.Fatalf("unexpected enrollment timeout %s", cfg.ZitiEnrollmentTimeout)
	}
	if cfg.ZitiLeaseRenewalInterval != 2*time.Minute {
		t.Fatalf("unexpected lease renewal interval %s", cfg.ZitiLeaseRenewalInterval)
	}
}

func TestFromEnvOverridesZitiIdentityAcquisition(t *testing.T) {
	t.Setenv("TERMINAL_PROXY_TICKET_SIGNING_KEY", "01234567890123456789012345678901!")
	t.Setenv("ZITI_ENABLED", "true")
	t.Setenv("ZITI_MANAGEMENT_ADDRESS", "ziti-management.platform:50051")
	t.Setenv("ZITI_ENROLLMENT_TIMEOUT", "30s")
	t.Setenv("ZITI_LEASE_RENEWAL_INTERVAL", "45s")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("from env: %v", err)
	}
	if cfg.ZitiManagementAddress != "ziti-management.platform:50051" {
		t.Fatalf("unexpected ziti management address %q", cfg.ZitiManagementAddress)
	}
	if cfg.ZitiEnrollmentTimeout != 30*time.Second {
		t.Fatalf("unexpected enrollment timeout %s", cfg.ZitiEnrollmentTimeout)
	}
	if cfg.ZitiLeaseRenewalInterval != 45*time.Second {
		t.Fatalf("unexpected lease renewal interval %s", cfg.ZitiLeaseRenewalInterval)
	}
}

// The overlay is the only path that reaches a runner outside this cluster, so a
// non-positive interval must fail startup rather than silently leaving the
// identity's lease to lapse.
func TestFromEnvRejectsNonPositiveLeaseRenewalInterval(t *testing.T) {
	t.Setenv("TERMINAL_PROXY_TICKET_SIGNING_KEY", "01234567890123456789012345678901!")
	t.Setenv("ZITI_ENABLED", "true")
	t.Setenv("ZITI_LEASE_RENEWAL_INTERVAL", "0s")

	if _, err := FromEnv(); err == nil {
		t.Fatal("expected a non-positive lease renewal interval to be rejected")
	}
}
