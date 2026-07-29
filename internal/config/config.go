package config

import (
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddress           string
	GRPCAddress           string
	PublicWebSocketURL    string
	TicketSigningKey      []byte
	TicketTTL             time.Duration
	TouchInterval         time.Duration
	WebSocketPingInterval time.Duration
	WebSocketPingTimeout  time.Duration
	RunnersAddress        string
	AgentsAddress         string
	AuthorizationAddress  string
	RunnerAddress            string
	ZitiEnabled              bool
	ZitiManagementAddress    string
	ZitiEnrollmentTimeout    time.Duration
	ZitiLeaseRenewalInterval time.Duration
}

func FromEnv() (Config, error) {
	cfg := Config{
		HTTPAddress:           envDefault("HTTP_ADDRESS", ":8080"),
		GRPCAddress:           envDefault("GRPC_ADDRESS", ":50051"),
		PublicWebSocketURL:    envDefault("TERMINAL_PROXY_WEBSOCKET_URL", "ws://localhost:8080/terminal"),
		TicketTTL:             30 * time.Second,
		TouchInterval:         10 * time.Second,
		WebSocketPingInterval: 30 * time.Second,
		WebSocketPingTimeout:  30 * time.Second,
		RunnersAddress:        envDefault("RUNNERS_ADDRESS", "runners:50051"),
		AgentsAddress:         envDefault("AGENTS_ADDRESS", "agents:50051"),
		AuthorizationAddress:  envDefault("AUTHORIZATION_ADDRESS", "authorization:50051"),
		RunnerAddress:         envDefault("RUNNER_ADDRESS", "k8s-runner:50051"),
	}
	key, err := signingKeyFromEnv(os.Getenv("TERMINAL_PROXY_TICKET_SIGNING_KEY"))
	if err != nil {
		return Config{}, err
	}
	cfg.TicketSigningKey = key
	if value := strings.TrimSpace(os.Getenv("TERMINAL_PROXY_TICKET_TTL")); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse TERMINAL_PROXY_TICKET_TTL: %w", err)
		}
		cfg.TicketTTL = parsed
	}
	if cfg.TicketTTL <= 0 {
		return Config{}, fmt.Errorf("TERMINAL_PROXY_TICKET_TTL must be greater than 0")
	}
	if value := strings.TrimSpace(os.Getenv("TERMINAL_PROXY_TOUCH_INTERVAL")); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse TERMINAL_PROXY_TOUCH_INTERVAL: %w", err)
		}
		cfg.TouchInterval = parsed
	}
	if cfg.TouchInterval <= 0 {
		return Config{}, fmt.Errorf("TERMINAL_PROXY_TOUCH_INTERVAL must be greater than 0")
	}
	if value := strings.TrimSpace(os.Getenv("TERMINAL_PROXY_WEBSOCKET_PING_INTERVAL")); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse TERMINAL_PROXY_WEBSOCKET_PING_INTERVAL: %w", err)
		}
		cfg.WebSocketPingInterval = parsed
	}
	if cfg.WebSocketPingInterval <= 0 {
		return Config{}, fmt.Errorf("TERMINAL_PROXY_WEBSOCKET_PING_INTERVAL must be greater than 0")
	}
	if value := strings.TrimSpace(os.Getenv("TERMINAL_PROXY_WEBSOCKET_PING_TIMEOUT")); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse TERMINAL_PROXY_WEBSOCKET_PING_TIMEOUT: %w", err)
		}
		cfg.WebSocketPingTimeout = parsed
	}
	if cfg.WebSocketPingTimeout <= 0 {
		return Config{}, fmt.Errorf("TERMINAL_PROXY_WEBSOCKET_PING_TIMEOUT must be greater than 0")
	}
	if value := strings.TrimSpace(os.Getenv("ZITI_ENABLED")); value != "" {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse ZITI_ENABLED: %w", err)
		}
		cfg.ZitiEnabled = parsed
	}
	if cfg.ZitiEnabled {
		cfg.ZitiManagementAddress = envDefault("ZITI_MANAGEMENT_ADDRESS", "ziti-management:50051")
		cfg.ZitiEnrollmentTimeout = 2 * time.Minute
		if value := strings.TrimSpace(os.Getenv("ZITI_ENROLLMENT_TIMEOUT")); value != "" {
			parsed, err := time.ParseDuration(value)
			if err != nil {
				return Config{}, fmt.Errorf("parse ZITI_ENROLLMENT_TIMEOUT: %w", err)
			}
			cfg.ZitiEnrollmentTimeout = parsed
		}
		if cfg.ZitiEnrollmentTimeout <= 0 {
			return Config{}, fmt.Errorf("ZITI_ENROLLMENT_TIMEOUT must be greater than 0")
		}
		cfg.ZitiLeaseRenewalInterval = 2 * time.Minute
		if value := strings.TrimSpace(os.Getenv("ZITI_LEASE_RENEWAL_INTERVAL")); value != "" {
			parsed, err := time.ParseDuration(value)
			if err != nil {
				return Config{}, fmt.Errorf("parse ZITI_LEASE_RENEWAL_INTERVAL: %w", err)
			}
			cfg.ZitiLeaseRenewalInterval = parsed
		}
		if cfg.ZitiLeaseRenewalInterval <= 0 {
			return Config{}, fmt.Errorf("ZITI_LEASE_RENEWAL_INTERVAL must be greater than 0")
		}
	}
	return cfg, nil
}

func envDefault(name, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func signingKeyFromEnv(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, fmt.Errorf("TERMINAL_PROXY_TICKET_SIGNING_KEY is required")
	}
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err == nil {
		if len(decoded) < 32 {
			return nil, fmt.Errorf("TERMINAL_PROXY_TICKET_SIGNING_KEY must be at least 32 bytes")
		}
		return decoded, nil
	}
	key := []byte(value)
	if len(key) < 32 {
		return nil, fmt.Errorf("TERMINAL_PROXY_TICKET_SIGNING_KEY must be at least 32 bytes")
	}
	return key, nil
}
