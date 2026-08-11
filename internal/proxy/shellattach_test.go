package proxy

import (
	"encoding/json"
	"testing"

	runnerv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/runner/v1"
	terminalproxyv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/terminal_proxy/v1"
	"github.com/agynio/terminal-proxy/internal/ticket"
)

// A displaced client and a shell that ended both reach the runner as an
// ordinary completion. Without the code the bound command reports, a client
// taken over on another device would be told its shell exited.
func TestExitReasonDistinguishesDisplacement(t *testing.T) {
	for _, tc := range []struct {
		name       string
		kind       terminalproxyv1.SessionKind
		reason     runnerv1.ExecExitReason
		code       int32
		wantReason string
		wantCode   int32
	}{
		{
			name:       "displaced",
			kind:       terminalproxyv1.SessionKind_SESSION_KIND_SHELL_ATTACH,
			reason:     runnerv1.ExecExitReason_EXEC_EXIT_REASON_COMPLETED,
			code:       ticket.ExitDisplaced,
			wantReason: "displaced",
			// Normalized: reporting 91 would name a status the shell never
			// produced.
			wantCode: 0,
		},
		{
			name:       "shell exited",
			kind:       terminalproxyv1.SessionKind_SESSION_KIND_SHELL_ATTACH,
			reason:     runnerv1.ExecExitReason_EXEC_EXIT_REASON_COMPLETED,
			code:       0,
			wantReason: "completed",
			wantCode:   0,
		},
		{
			name:       "shell exited nonzero",
			kind:       terminalproxyv1.SessionKind_SESSION_KIND_SHELL_ATTACH,
			reason:     runnerv1.ExecExitReason_EXEC_EXIT_REASON_COMPLETED,
			code:       2,
			wantReason: "completed",
			wantCode:   2,
		},
		{
			// The code means nothing for a kind whose command does not report
			// it, and an ephemeral shell exiting 91 is just an exit status.
			name:       "same code on an ephemeral shell",
			kind:       terminalproxyv1.SessionKind_SESSION_KIND_SHELL,
			reason:     runnerv1.ExecExitReason_EXEC_EXIT_REASON_COMPLETED,
			code:       ticket.ExitDisplaced,
			wantReason: "completed",
			wantCode:   ticket.ExitDisplaced,
		},
		{
			name:       "workload stopped mid-session",
			kind:       terminalproxyv1.SessionKind_SESSION_KIND_SHELL_ATTACH,
			reason:     runnerv1.ExecExitReason_EXEC_EXIT_REASON_CANCELLED,
			code:       ticket.ExitDisplaced,
			wantReason: "cancelled",
			wantCode:   ticket.ExitDisplaced,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reason, code := exitReason(tc.reason, tc.code, tc.kind)
			if reason != tc.wantReason || code != tc.wantCode {
				t.Fatalf("got (%s, %d), want (%s, %d)", reason, code, tc.wantReason, tc.wantCode)
			}
		})
	}
}

func TestHandshakeTerm(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		want    string
		wantErr bool
	}{
		// Nothing sent one before this field existed, and a client that names
		// none is not an error — it is one that did not look.
		{name: "absent defaults", payload: `{"cols":80,"rows":24}`, want: defaultTerm},
		{name: "blank defaults", payload: `{"cols":80,"rows":24,"term":"  "}`, want: defaultTerm},
		{name: "carried through", payload: `{"cols":80,"rows":24,"term":"xterm-kitty"}`, want: "xterm-kitty"},
		// It becomes an environment variable in the container, so it is held
		// to the shape a terminal name actually has.
		{name: "injection rejected", payload: `{"cols":80,"rows":24,"term":"x;rm -rf /"}`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hs handshake
			if err := json.Unmarshal([]byte(tc.payload), &hs); err != nil {
				t.Fatal(err)
			}
			got, err := normalizeHandshake(hs)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("accepted %q", tc.payload)
				}
				return
			}
			if err != nil {
				t.Fatalf("rejected %q: %v", tc.payload, err)
			}
			if got.Term != tc.want {
				t.Fatalf("term = %q, want %q", got.Term, tc.want)
			}
		})
	}
}

// TERM describes a terminal, and SYNC has none — it speaks a binary protocol
// to a process that never renders anything.
func TestStartRequestSetsTermOnlyForTTY(t *testing.T) {
	hs := handshake{Cols: 80, Rows: 24, Term: "xterm-kitty"}

	tty := startRequest(&ticket.Claims{ID: "a", TTY: true, Env: []string{"TMUX_TMPDIR=/agyn/run"}}, "target", hs)
	if !hasEnv(tty.GetOptions().GetEnv(), "TERM", "xterm-kitty") {
		t.Fatal("TERM not passed to a TTY session")
	}
	if !hasEnv(tty.GetOptions().GetEnv(), "TMUX_TMPDIR", "/agyn/run") {
		t.Fatal("ticket environment not passed through")
	}

	sync := startRequest(&ticket.Claims{ID: "b", TTY: false}, "target", hs)
	for _, kv := range sync.GetOptions().GetEnv() {
		if kv.GetName() == "TERM" {
			t.Fatal("TERM passed to a non-TTY session")
		}
	}
}

func hasEnv(env []*runnerv1.EnvVar, name, value string) bool {
	for _, kv := range env {
		if kv.GetName() == name && kv.GetValue() == value {
			return true
		}
	}
	return false
}