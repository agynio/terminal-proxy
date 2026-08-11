package ticket

import (
	"strings"
	"testing"

	terminalproxyv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/terminal_proxy/v1"
)

func resolveAttach(t *testing.T, shellID, cwd string) *Command {
	t.Helper()
	command, err := Resolve(terminalproxyv1.SessionKind_SESSION_KIND_SHELL_ATTACH, nil, "", shellID, cwd)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	return command
}

// One command has to cover both attaching and creating, because that is what
// makes reconnecting and restoring-after-a-restart the same path for a client
// that cannot tell which case it is in.
func TestShellAttachIsAttachOrCreate(t *testing.T) {
	command := resolveAttach(t, "shell-a", "")

	if !command.TTY {
		t.Fatal("SHELL_ATTACH must allocate a PTY")
	}
	for _, want := range []string{"new-session", "-A", "-D", "'shell-a'"} {
		if !strings.Contains(command.Shell, want) {
			t.Fatalf("command missing %q: %s", want, command.Shell)
		}
	}
}

// The directory is applied only on creation, so a client may pass its stored
// value on every attach without moving a shell someone is working in.
func TestShellAttachCwdIsQuotedAndOptional(t *testing.T) {
	if got := resolveAttach(t, "shell-a", ""); strings.Contains(got.Shell, " -c ") {
		t.Fatalf("no cwd was given but one was bound: %s", got.Shell)
	}
	if got := resolveAttach(t, "shell-a", "/workspace/api/"); !strings.Contains(got.Shell, "-c '/workspace/api'") {
		t.Fatalf("cwd not bound normalized and quoted: %s", got.Shell)
	}
}

// Without TMUX_TMPDIR the client looks in the image's /tmp, finds no server,
// and starts a second one — on a path that may be read-only and carrying
// whatever environment that first client happened to have.
func TestShellAttachBindsSocketDirectory(t *testing.T) {
	command := resolveAttach(t, "shell-a", "")

	var found bool
	for _, kv := range command.Env {
		if kv == "TMUX_TMPDIR="+tmuxTmpDir {
			found = true
		}
	}
	if !found {
		t.Fatalf("TMUX_TMPDIR not bound: %v", command.Env)
	}
}

// tmux resolves a target by prefix, so a lookup that is not anchored can
// answer about a different shell whose name this one begins.
func TestShellAttachAnchorsTheSurvivalCheck(t *testing.T) {
	command := resolveAttach(t, "shell-a", "")

	if !strings.Contains(command.Shell, "has-session -t '=shell-a'") {
		t.Fatalf("survival check not anchored to the whole name: %s", command.Shell)
	}
	if !strings.Contains(command.Shell, "exit 91") {
		t.Fatalf("displaced exit code not reported: %s", command.Shell)
	}
}

func TestValidateShellID(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   string
		ok   bool
	}{
		{"opaque uuid", "0b6f1f4a-6d5a-4a1e-9a0e-2c7f5d3b1a44", true},
		{"underscores", "shell_1", true},
		{"empty", "", false},
		// tmux reads both as target separators, so either addresses something
		// other than the shell the caller asked for.
		{"dot", "shell.a", false},
		{"colon", "shell:a", false},
		{"space", "shell a", false},
		{"too long", strings.Repeat("a", 65), false},
		{"shell metacharacter", "shell$(id)", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateShellID(tc.id)
			if tc.ok && err != nil {
				t.Fatalf("rejected %q: %v", tc.id, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("accepted %q", tc.id)
			}
		})
	}
}

// The value is interpolated into a shell command, so a quote in it would end
// the string it was meant to sit inside.
func TestValidateShellCwd(t *testing.T) {
	for _, tc := range []struct {
		name string
		cwd  string
		ok   bool
	}{
		{"absolute", "/workspace", true},
		{"empty is allowed", "", true},
		{"relative", "workspace", false},
		{"traversal", "/workspace/../etc", false},
		{"single quote", "/work'space", false},
		{"newline", "/work\nspace", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateShellCwd(tc.cwd)
			if tc.ok && err != nil {
				t.Fatalf("rejected %q: %v", tc.cwd, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("accepted %q", tc.cwd)
			}
		})
	}
}

// A parameter that means nothing for a kind must be refused rather than
// ignored: silently dropping it is how a caller believes it asked for
// something it did not get.
func TestShellParametersRejectedOnOtherKinds(t *testing.T) {
	for _, kind := range []terminalproxyv1.SessionKind{
		terminalproxyv1.SessionKind_SESSION_KIND_SHELL,
		terminalproxyv1.SessionKind_SESSION_KIND_SYNC,
		terminalproxyv1.SessionKind_SESSION_KIND_EXEC,
	} {
		if _, err := Resolve(kind, nil, "/workspace", "shell-a", ""); err == nil {
			t.Fatalf("%s accepted shell_id", kind)
		}
	}
}

func TestShellAttachRequiresAnID(t *testing.T) {
	if _, err := Resolve(terminalproxyv1.SessionKind_SESSION_KIND_SHELL_ATTACH, nil, "", "", ""); err == nil {
		t.Fatal("accepted SHELL_ATTACH with no shell_id")
	}
}
