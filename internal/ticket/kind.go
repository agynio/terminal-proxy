package ticket

import (
	"errors"
	"fmt"
	"path"
	"strings"

	terminalproxyv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/terminal_proxy/v1"
)

// platformBinDir is where the binary init containers deliver agyn and agynd.
// Nothing in the container puts it on PATH — agynd extends PATH only for the
// agent subprocess it spawns, and a sandbox does not run agynd — so a session's
// command establishes it.
const platformBinDir = "/agyn/bin"

// syncEndpoint is invoked by absolute path and depends on PATH not at all,
// which is what a machine-invoked command should do.
const syncEndpoint = platformBinDir + "/agyn"

// PathPrefix puts the platform binaries in front for a command that does not
// run a login profile. Expanded in the container, so the image's own PATH
// survives.
const PathPrefix = `export PATH="` + platformBinDir + `:$PATH"; `

// DefaultShell runs the login profile first and prepends afterwards.
// /etc/profile assigns PATH outright on Debian and Alpine, so a prefix set
// ahead of a login shell is discarded. The final shell is not a login shell as
// a result: /etc/profile and ~/.profile have run, ~/.bash_profile has not.
const DefaultShell = `exec /bin/sh -lc 'PATH=` + platformBinDir + `:$PATH exec ${SHELL:-sh}'`

// Command is what a kind resolves to.
type Command struct {
	Shell string
	Argv  []string
	// TTY is false only for SYNC, which carries a binary protocol that a PTY
	// would corrupt by merging stderr in and translating line endings.
	TTY bool
}

// Resolve maps a session kind to the command the ticket binds. The mapping
// lives here rather than in the Gateway because it is terminal-session domain
// logic; the Gateway routes and does not interpret.
func Resolve(kind terminalproxyv1.SessionKind, command *terminalproxyv1.TerminalCommand, syncRoot string) (*Command, error) {
	switch kind {
	case terminalproxyv1.SessionKind_SESSION_KIND_SHELL:
		if err := rejectUnused(command, syncRoot); err != nil {
			return nil, err
		}
		return &Command{Shell: DefaultShell, TTY: true}, nil

	case terminalproxyv1.SessionKind_SESSION_KIND_EXEC:
		if strings.TrimSpace(syncRoot) != "" {
			return nil, errors.New("sync_root is meaningful only for SYNC")
		}
		return execCommand(command)

	case terminalproxyv1.SessionKind_SESSION_KIND_SYNC:
		if command != nil {
			return nil, errors.New("command is meaningful only for EXEC")
		}
		root, err := ValidateSyncRoot(syncRoot)
		if err != nil {
			return nil, err
		}
		return &Command{Argv: []string{syncEndpoint, "sandbox", "sync", "serve", "--root", root}}, nil

	case terminalproxyv1.SessionKind_SESSION_KIND_UNSPECIFIED:
		// Never defaulted. Silently defaulting the field that selects a command
		// is how an unintended command ends up in a ticket.
		return nil, errors.New("kind is required")

	default:
		return nil, fmt.Errorf("unsupported session kind %s", kind)
	}
}

func rejectUnused(command *terminalproxyv1.TerminalCommand, syncRoot string) error {
	if command != nil {
		return errors.New("command is meaningful only for EXEC")
	}
	if strings.TrimSpace(syncRoot) != "" {
		return errors.New("sync_root is meaningful only for SYNC")
	}
	return nil
}

// execCommand carries a caller-supplied command. It is authorized by the same
// check as SHELL — binding at issuance buys immutability, not permission.
func execCommand(command *terminalproxyv1.TerminalCommand) (*Command, error) {
	if command == nil {
		return nil, errors.New("command is required for EXEC")
	}
	switch value := command.GetCommand().(type) {
	case *terminalproxyv1.TerminalCommand_Shell:
		shell := strings.TrimSpace(value.Shell)
		if shell == "" {
			return nil, errors.New("command shell is empty")
		}
		return &Command{Shell: PathPrefix + shell, TTY: true}, nil
	case *terminalproxyv1.TerminalCommand_Argv:
		argv := cleanArgv(value.Argv.GetArgv())
		if len(argv) == 0 {
			return nil, errors.New("command argv is empty")
		}
		return &Command{Argv: argv, TTY: true}, nil
	default:
		return nil, errors.New("command is required for EXEC")
	}
}

// ValidateSyncRoot is the lexical half of root validation: absolute,
// normalized, no traversal. The other half — resolving symlinks and confining
// the result to the workspace mount — can only happen inside the container,
// where the filesystem is visible.
func ValidateSyncRoot(root string) (string, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return "", errors.New("sync_root is required for SYNC")
	}
	if !strings.HasPrefix(root, "/") {
		return "", fmt.Errorf("sync_root %q is not absolute", root)
	}
	// Checked before cleaning, not after: path.Clean resolves "/workspace/../etc"
	// to "/etc", so a check on the cleaned form never sees the traversal it was
	// meant to catch.
	for _, segment := range strings.Split(root, "/") {
		if segment == ".." {
			return "", fmt.Errorf("sync_root %q traverses upward", root)
		}
	}
	return path.Clean(root), nil
}

func cleanArgv(argv []string) []string {
	cleaned := make([]string, 0, len(argv))
	for _, arg := range argv {
		if strings.TrimSpace(arg) == "" {
			continue
		}
		cleaned = append(cleaned, arg)
	}
	return cleaned
}
