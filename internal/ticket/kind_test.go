package ticket

import (
	"strings"
	"testing"

	terminalproxyv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/terminal_proxy/v1"
)

func TestSyncResolvesToTheEndpointByAbsolutePath(t *testing.T) {
	command, err := Resolve(terminalproxyv1.SessionKind_SESSION_KIND_SYNC, nil, "/workspace/project")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if command.TTY {
		t.Fatal("SYNC must not allocate a PTY: it would merge stderr in and translate line endings")
	}
	// Nothing puts /agyn/bin on PATH for an exec'd process, so a machine-invoked
	// command must not depend on it.
	if len(command.Argv) == 0 || command.Argv[0] != "/agyn/bin/agyn" {
		t.Fatalf("expected an absolute invocation, got %v", command.Argv)
	}
	if command.Shell != "" {
		t.Fatalf("SYNC must not go through a shell, got %q", command.Shell)
	}
	joined := strings.Join(command.Argv, " ")
	if !strings.Contains(joined, "sandbox sync serve --root /workspace/project") {
		t.Fatalf("unexpected command %q", joined)
	}
}

// Silently defaulting the field that selects a command is how an unintended
// command ends up in a ticket.
func TestUnspecifiedKindIsRejected(t *testing.T) {
	if _, err := Resolve(terminalproxyv1.SessionKind_SESSION_KIND_UNSPECIFIED, nil, ""); err == nil {
		t.Fatal("expected an unspecified kind to be rejected")
	}
}

func TestShellRunsTheProfileBeforePrependingPath(t *testing.T) {
	command, err := Resolve(terminalproxyv1.SessionKind_SESSION_KIND_SHELL, nil, "")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !command.TTY {
		t.Fatal("SHELL must allocate a PTY")
	}
	// /etc/profile assigns PATH outright on Debian and Alpine, so a prefix set
	// ahead of the login shell is discarded.
	if strings.HasPrefix(command.Shell, PathPrefix) {
		t.Fatalf("PATH is set ahead of the login shell: %q", command.Shell)
	}
	if !strings.Contains(command.Shell, "-lc") {
		t.Fatalf("the login profile is not run: %q", command.Shell)
	}
}

// A kind's parameters belong to that kind. Accepting them elsewhere would let a
// ticket describe something its kind does not mean.
func TestParametersAreRejectedOnTheWrongKind(t *testing.T) {
	shellCmd := &terminalproxyv1.TerminalCommand{
		Command: &terminalproxyv1.TerminalCommand_Shell{Shell: "echo hi"},
	}
	if _, err := Resolve(terminalproxyv1.SessionKind_SESSION_KIND_SHELL, shellCmd, ""); err == nil {
		t.Fatal("expected a command on SHELL to be rejected")
	}
	if _, err := Resolve(terminalproxyv1.SessionKind_SESSION_KIND_SYNC, shellCmd, "/workspace"); err == nil {
		t.Fatal("expected a command on SYNC to be rejected")
	}
	if _, err := Resolve(terminalproxyv1.SessionKind_SESSION_KIND_EXEC, shellCmd, "/workspace"); err == nil {
		t.Fatal("expected a sync_root on EXEC to be rejected")
	}
	if _, err := Resolve(terminalproxyv1.SessionKind_SESSION_KIND_SHELL, nil, "/workspace"); err == nil {
		t.Fatal("expected a sync_root on SHELL to be rejected")
	}
}

// The lexical half of root validation. Resolving symlinks and confining to the
// workspace mount can only happen inside the container.
func TestSyncRootLexicalValidation(t *testing.T) {
	for _, root := range []string{"", "relative/path", "/workspace/../etc", "/../etc"} {
		if _, err := ValidateSyncRoot(root); err == nil {
			t.Fatalf("expected %q to be rejected", root)
		}
	}
	cleaned, err := ValidateSyncRoot("/workspace/./project/")
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cleaned != "/workspace/project" {
		t.Fatalf("expected a normalized root, got %q", cleaned)
	}
}

func TestExecRequiresACommand(t *testing.T) {
	if _, err := Resolve(terminalproxyv1.SessionKind_SESSION_KIND_EXEC, nil, ""); err == nil {
		t.Fatal("expected EXEC without a command to be rejected")
	}
	empty := &terminalproxyv1.TerminalCommand{
		Command: &terminalproxyv1.TerminalCommand_Argv{Argv: &terminalproxyv1.TerminalArgv{Argv: []string{"  "}}},
	}
	if _, err := Resolve(terminalproxyv1.SessionKind_SESSION_KIND_EXEC, empty, ""); err == nil {
		t.Fatal("expected EXEC with an empty argv to be rejected")
	}
}
