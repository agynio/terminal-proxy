package ticket

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	terminalproxyv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/terminal_proxy/v1"
	"github.com/google/uuid"
)

const version = 1

type Store struct {
	key  []byte
	now  func() time.Time
	mu   sync.Mutex
	used map[string]time.Time
}

type Claims struct {
	Version       int      `json:"v"`
	ID            string   `json:"jti"`
	IdentityID    string   `json:"sub"`
	WorkloadID    string   `json:"workload_id"`
	ContainerName string   `json:"container_name"`
	CommandShell  string   `json:"command_shell,omitempty"`
	CommandArgv   []string `json:"command_argv,omitempty"`
	// Env is bound with the command and for the same reason: what the session
	// runs must not be changeable at attach time.
	Env  []string `json:"env,omitempty"`
	Kind int32    `json:"kind"`
	TTY           bool     `json:"tty"`
	IssuedAt      int64    `json:"iat"`
	ExpiresAt     int64    `json:"exp"`
}

func NewStore(key []byte) *Store {
	copied := append([]byte(nil), key...)
	return &Store{key: copied, now: time.Now, used: map[string]time.Time{}}
}

func (s *Store) Issue(identityID, workloadID, containerName string, kind terminalproxyv1.SessionKind, command *Command, ttl time.Duration) (string, time.Time, error) {
	if len(s.key) < 32 {
		return "", time.Time{}, errors.New("ticket signing key must be at least 32 bytes")
	}
	identityID, err := parseUUID(identityID, "identity_id")
	if err != nil {
		return "", time.Time{}, err
	}
	workloadID, err = parseUUID(workloadID, "workload_id")
	if err != nil {
		return "", time.Time{}, err
	}
	containerName = strings.TrimSpace(containerName)
	if containerName == "" {
		return "", time.Time{}, errors.New("container_name is required")
	}
	if ttl <= 0 {
		return "", time.Time{}, errors.New("ticket ttl must be greater than 0")
	}
	if command == nil {
		return "", time.Time{}, errors.New("command is required")
	}
	claims := Claims{
		Version:       version,
		ID:            uuid.NewString(),
		IdentityID:    identityID,
		WorkloadID:    workloadID,
		ContainerName: containerName,
		CommandShell:  command.Shell,
		CommandArgv:   command.Argv,
		Env:           command.Env,
		Kind:          int32(kind),
		TTY:           command.TTY,
	}
	now := s.now().UTC()
	claims.IssuedAt = now.Unix()
	expiresAt := now.Add(ttl)
	claims.ExpiresAt = expiresAt.Unix()
	token, err := s.sign(claims)
	return token, expiresAt, err
}

func (s *Store) Consume(raw string) (*Claims, error) {
	claims, err := s.verify(raw)
	if err != nil {
		return nil, err
	}
	now := s.now().UTC()
	if !now.Before(time.Unix(claims.ExpiresAt, 0)) {
		return nil, errors.New("ticket expired")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeLocked(now)
	if _, ok := s.used[claims.ID]; ok {
		return nil, errors.New("ticket already used")
	}
	s.used[claims.ID] = time.Unix(claims.ExpiresAt, 0)
	return claims, nil
}

func (s *Store) sign(claims Claims) (string, error) {
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(encodedPayload))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return encodedPayload + "." + signature, nil
}

func (s *Store) verify(raw string) (*Claims, error) {
	parts := strings.Split(raw, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, errors.New("ticket malformed")
	}
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(parts[0]))
	expected := mac.Sum(nil)
	actual, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, errors.New("ticket signature malformed")
	}
	if !hmac.Equal(actual, expected) {
		return nil, errors.New("ticket signature invalid")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, errors.New("ticket payload malformed")
	}
	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, errors.New("ticket payload invalid")
	}
	if claims.Version != version || claims.ID == "" || claims.IdentityID == "" || claims.WorkloadID == "" || claims.ContainerName == "" || claims.ExpiresAt == 0 {
		return nil, errors.New("ticket claims invalid")
	}
	if claims.CommandShell == "" && len(claims.CommandArgv) == 0 {
		return nil, errors.New("ticket names no command")
	}
	return &claims, nil
}

func (s *Store) purgeLocked(now time.Time) {
	for id, expiresAt := range s.used {
		if !now.Before(expiresAt) {
			delete(s.used, id)
		}
	}
}

func parseUUID(value, field string) (string, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(value))
	if err != nil {
		return "", fmt.Errorf("%s must be a UUID", field)
	}
	return parsed.String(), nil
}

func RandomKey() []byte {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	return key
}

func ContextWithIdentity(ctx context.Context, identityID string) context.Context {
	return context.WithValue(ctx, identityContextKey{}, identityID)
}

type identityContextKey struct{}
