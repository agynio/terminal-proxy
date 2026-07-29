package ticket

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	terminalproxyv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/terminal_proxy/v1"
	"github.com/google/uuid"
)

const (
	testKey      = "01234567890123456789012345678901"
	testOtherKey = "abcdefghijklmnopqrstuvwxyz012345"
)

func TestIssueAndConsumeRoundTrip(t *testing.T) {
	store := NewStore([]byte(testKey))
	identityID := uuid.NewString()
	workloadID := uuid.NewString()

	token, expiresAt, err := store.Issue(identityID, workloadID, "main", nil, 30*time.Second)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if !expiresAt.After(time.Now()) {
		t.Fatalf("expected future expiry, got %s", expiresAt)
	}
	claims, err := store.Consume(token)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if claims.IdentityID != identityID || claims.WorkloadID != workloadID || claims.ContainerName != "main" {
		t.Fatalf("unexpected claims: %#v", claims)
	}
	if claims.CommandShell != "exec ${SHELL:-sh} -l" {
		t.Fatalf("unexpected default command %q", claims.CommandShell)
	}
	if claims.Version != version || claims.ID == "" {
		t.Fatalf("unexpected envelope: %#v", claims)
	}
}

func TestIssueBindsCommandOverride(t *testing.T) {
	store := NewStore([]byte(testKey))
	command := &terminalproxyv1.TerminalCommand{
		Command: &terminalproxyv1.TerminalCommand_Argv{Argv: &terminalproxyv1.TerminalArgv{Argv: []string{"cat", "/etc/hosts"}}},
	}

	token, _, err := store.Issue(uuid.NewString(), uuid.NewString(), "main", command, 30*time.Second)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	claims, err := store.Consume(token)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if len(claims.CommandArgv) != 2 || claims.CommandArgv[0] != "cat" || claims.CommandArgv[1] != "/etc/hosts" {
		t.Fatalf("unexpected argv %v", claims.CommandArgv)
	}
	if claims.CommandShell != "" {
		t.Fatalf("expected no shell command, got %q", claims.CommandShell)
	}
}

func TestConsumeRejectsTamperedSignature(t *testing.T) {
	store := NewStore([]byte(testKey))
	token, _, err := store.Issue(uuid.NewString(), uuid.NewString(), "main", nil, 30*time.Second)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		t.Fatalf("unexpected token shape %q", token)
	}

	if _, err := store.Consume(parts[0] + "." + flipLastByte(t, parts[1])); err == nil {
		t.Fatal("expected tampered signature to be rejected")
	}
	// A payload edited to point at another workload keeps the original
	// signature, which must no longer verify.
	if _, err := store.Consume(retargetPayload(t, token, uuid.NewString())); err == nil {
		t.Fatal("expected tampered payload to be rejected")
	}
	if _, err := store.Consume("not-a-ticket"); err == nil {
		t.Fatal("expected malformed ticket to be rejected")
	}
}

func TestConsumeRejectsExpiredTicket(t *testing.T) {
	store := NewStore([]byte(testKey))
	issuedAt := time.Now().UTC()
	store.now = func() time.Time { return issuedAt }

	token, _, err := store.Issue(uuid.NewString(), uuid.NewString(), "main", nil, 30*time.Second)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	store.now = func() time.Time { return issuedAt.Add(29 * time.Second) }
	if _, err := store.Consume(token); err != nil {
		t.Fatalf("consume before expiry: %v", err)
	}

	fresh, _, err := store.Issue(uuid.NewString(), uuid.NewString(), "main", nil, 30*time.Second)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	store.now = func() time.Time { return issuedAt.Add(90 * time.Second) }
	if _, err := store.Consume(fresh); err == nil {
		t.Fatal("expected expired ticket to be rejected")
	}
}

func TestConsumeRejectsReplay(t *testing.T) {
	store := NewStore([]byte(testKey))
	token, _, err := store.Issue(uuid.NewString(), uuid.NewString(), "main", nil, 30*time.Second)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if _, err := store.Consume(token); err != nil {
		t.Fatalf("first consume: %v", err)
	}
	if _, err := store.Consume(token); err == nil {
		t.Fatal("expected replayed ticket to be rejected")
	}
}

// The signed-ticket design exists so tickets survive horizontal scaling: any
// replica sharing the signing key validates a ticket issued by any other.
func TestTicketFromAnotherReplicaValidates(t *testing.T) {
	issuer := NewStore([]byte(testKey))
	validator := NewStore([]byte(testKey))
	identityID := uuid.NewString()
	workloadID := uuid.NewString()

	token, _, err := issuer.Issue(identityID, workloadID, "main", nil, 30*time.Second)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	claims, err := validator.Consume(token)
	if err != nil {
		t.Fatalf("consume on second replica: %v", err)
	}
	if claims.IdentityID != identityID || claims.WorkloadID != workloadID {
		t.Fatalf("unexpected claims: %#v", claims)
	}
	// Single-use is per replica: the issuing replica has not seen this ticket.
	if _, err := issuer.Consume(token); err != nil {
		t.Fatalf("issuing replica consume: %v", err)
	}
}

func TestTicketFromReplicaWithDifferentKeyFails(t *testing.T) {
	issuer := NewStore([]byte(testKey))
	validator := NewStore([]byte(testOtherKey))

	token, _, err := issuer.Issue(uuid.NewString(), uuid.NewString(), "main", nil, 30*time.Second)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if _, err := validator.Consume(token); err == nil {
		t.Fatal("expected ticket signed with a different key to be rejected")
	}
}

func TestIssueRejectsInvalidInput(t *testing.T) {
	store := NewStore([]byte(testKey))
	if _, _, err := store.Issue("not-a-uuid", uuid.NewString(), "main", nil, 30*time.Second); err == nil {
		t.Fatal("expected invalid identity to be rejected")
	}
	if _, _, err := store.Issue(uuid.NewString(), "not-a-uuid", "main", nil, 30*time.Second); err == nil {
		t.Fatal("expected invalid workload to be rejected")
	}
	if _, _, err := store.Issue(uuid.NewString(), uuid.NewString(), "  ", nil, 30*time.Second); err == nil {
		t.Fatal("expected empty container name to be rejected")
	}
	if _, _, err := store.Issue(uuid.NewString(), uuid.NewString(), "main", nil, 0); err == nil {
		t.Fatal("expected non-positive ttl to be rejected")
	}
	short := NewStore([]byte("too-short"))
	if _, _, err := short.Issue(uuid.NewString(), uuid.NewString(), "main", nil, 30*time.Second); err == nil {
		t.Fatal("expected short signing key to be rejected")
	}
}

func flipLastByte(t *testing.T, encoded string) string {
	t.Helper()
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	decoded[len(decoded)-1] ^= 0xff
	return base64.RawURLEncoding.EncodeToString(decoded)
}

func retargetPayload(t *testing.T, token, workloadID string) string {
	t.Helper()
	parts := strings.Split(token, ".")
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatalf("parse payload: %v", err)
	}
	claims.WorkloadID = workloadID
	edited, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(edited) + "." + parts[1]
}
