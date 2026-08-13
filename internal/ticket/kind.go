package ticket

import (
	"errors"
	"fmt"
	"path"
	"regexp"
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
//
// LANG is defaulted rather than forced: an image that names its own locale
// keeps it. Without one glibc falls back to C, where every byte of a multi-byte
// keystroke is undecodable and zsh's line editor renders each as <ffffffff>.
const DefaultShell = `exec /bin/sh -lc 'PATH=` + platformBinDir + `:$PATH LANG=${LANG:-C.UTF-8} exec ${SHELL:-sh}'`

// tmux backs persistent shells. The proxy names it by absolute path and never
// puts it on a client's PATH: nothing above the container is meant to know
// what implements a shell.
const (
	tmuxBinary = platformBinDir + "/tmux"
	tmuxConfig = "/agyn/tmux.conf"

	// The platform's server, off the default socket, so an engineer running
	// their own tmux inside a shell gets their own server and their own
	// configuration rather than silently joining this one.
	tmuxSocket = "agyn"

	// Where agynd put the socket. Without this the client looks in the image's
	// /tmp, finds nothing, and starts a second server — on a path that may be
	// read-only and with whatever environment that first client happened to
	// carry.
	tmuxTmpDir = "/agyn/run"

	// Supplements the image's terminal database rather than replacing it; the
	// trailing empty element is what ncurses reads as "and the compiled-in
	// defaults too".
	tmuxTerminfoDirs = "/agyn/terminfo:"
)

// ExitDisplaced is the exit code a persistent shell's command uses to say the
// shell outlived this session.
//
// tmux exits 0 whether a client was detached or its shell ended, and the proxy
// cannot tell those apart from the exit alone. So the bound command asks: if
// the session still exists, this client was displaced by another. Nothing else
// can detach a client — the platform configuration binds no prefix key — so a
// surviving session means exactly one thing.
//
// Chosen above the 128+signal range so it cannot collide with a real death.
const ExitDisplaced = 91

// Command is what a kind resolves to.
type Command struct {
	Shell string
	Argv  []string
	// TTY is false only for SYNC, which carries a binary protocol that a PTY
	// would corrupt by merging stderr in and translating line endings.
	TTY bool
	// Persistent records that the bound command attaches to a shell that
	// outlives the session. The exit code alone cannot say so: an ephemeral
	// shell exiting 91 is just an exit status.
	Persistent bool
	// Env is bound into the ticket with the command, for the same reason the
	// command is: what the session runs must not be changeable at attach time.
	// KEY=VALUE form.
	Env []string
}

// Resolve maps a session kind to the command the ticket binds. The mapping
// lives here rather than in the Gateway because it is terminal-session domain
// logic; the Gateway routes and does not interpret.
//
// persistent says whether the target workload keeps its shells. It comes from
// the workload record, never from the caller: whether a shell outlives its
// connection is the environment's answer, and two clients against one sandbox
// must not be able to disagree about it.
func Resolve(kind terminalproxyv1.SessionKind, command *terminalproxyv1.TerminalCommand, syncRoot, shellID, shellCwd string, persistent bool) (*Command, error) {
	if kind != terminalproxyv1.SessionKind_SESSION_KIND_SHELL {
		if strings.TrimSpace(shellID) != "" || strings.TrimSpace(shellCwd) != "" {
			return nil, errors.New("shell_id and shell_cwd are meaningful only for SHELL")
		}
	}

	switch kind {
	case terminalproxyv1.SessionKind_SESSION_KIND_SHELL:
		if err := rejectUnused(command, syncRoot); err != nil {
			return nil, err
		}
		// Naming a shell where the workload does not keep them is not an
		// error. The caller asked for something this environment does not
		// provide, which is the environment's answer to give — and refusing it
		// would make every client read a setting it should not have to.
		if !persistent {
			return &Command{Shell: DefaultShell, TTY: true}, nil
		}
		if strings.TrimSpace(shellID) == "" {
			// A shell nobody named is a shell nobody will return to, so there
			// is nothing for persistence to buy.
			return &Command{Shell: DefaultShell, TTY: true}, nil
		}
		return shellAttachCommand(shellID, shellCwd)

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

// shellAttachCommand binds attach-or-create for one named shell.
//
// One command covers both cases on purpose: -A attaches when the session
// exists and creates it when it does not, which is what makes reconnecting and
// restoring-after-a-restart the same path. A caller passes its stored
// directory every time and -c applies it only on creation.
//
// -D displaces whatever else held the shell. One shell carries one
// attachment: it is what makes a page reload immediate rather than a wait for
// the previous client's missed pongs, and it removes the question of whose
// size a shared PTY takes.
func shellAttachCommand(shellID, shellCwd string) (*Command, error) {
	id, err := ValidateShellID(shellID)
	if err != nil {
		return nil, err
	}
	cwd, err := ValidateShellCwd(shellCwd)
	if err != nil {
		return nil, err
	}

	attach := []string{
		quote(tmuxBinary), "-L", tmuxSocket, "-f", quote(tmuxConfig),
		"new-session", "-A", "-D", "-s", quote(id),
	}
	if cwd != "" {
		attach = append(attach, "-c", quote(cwd))
	}

	// "=" anchors the lookup to the whole name; tmux otherwise resolves a
	// target by prefix, and one shell's id can be another's prefix.
	shell := fmt.Sprintf(
		`%s; rc=$?; %s -L %s has-session -t %s 2>/dev/null && exit %d; exit $rc`,
		strings.Join(attach, " "),
		quote(tmuxBinary), tmuxSocket, quote("="+id), ExitDisplaced,
	)

	return &Command{
		Shell:      shell,
		TTY:        true,
		Persistent: true,
		Env: []string{
			"TMUX_TMPDIR=" + tmuxTmpDir,
			"TERMINFO_DIRS=" + tmuxTerminfoDirs,
		},
	}, nil
}

var shellIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ValidateShellID keeps a ticket honest about what it names. '.' and ':' are
// excluded beyond what the pattern already rejects because tmux reads them as
// target separators, so a name carrying either addresses something other than
// the shell the caller asked for.
//
// This is not a privilege check. Anyone who may open a shell in a container
// can reach every shell in it from inside; what the id has to be is
// unambiguous, not secret.
func ValidateShellID(id string) (string, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", errors.New("shell_id is required")
	}
	if !shellIDPattern.MatchString(id) {
		return "", fmt.Errorf("shell_id %q must match %s", id, shellIDPattern)
	}
	return id, nil
}

// ValidateShellCwd is the lexical half of the same job ValidateSyncRoot does,
// with one addition: the value is interpolated into a shell command, so a
// quote in it would end the string it was meant to sit inside.
//
// Optional — a shell with no directory starts wherever the image puts it.
func ValidateShellCwd(cwd string) (string, error) {
	cwd = strings.TrimSpace(cwd)
	if cwd == "" {
		return "", nil
	}
	if err := checkAbsoluteNoTraversal(cwd, "shell_cwd"); err != nil {
		return "", err
	}
	if strings.ContainsAny(cwd, "'\x00\n\r") {
		return "", fmt.Errorf("shell_cwd %q contains a quote or control character", cwd)
	}
	return path.Clean(cwd), nil
}

func quote(value string) string { return "'" + value + "'" }

// ValidateSyncRoot is the lexical half of root validation: absolute,
// normalized, no traversal. The other half — resolving symlinks and confining
// the result to the workspace mount — can only happen inside the container,
// where the filesystem is visible.
func ValidateSyncRoot(root string) (string, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return "", errors.New("sync_root is required for SYNC")
	}
	if err := checkAbsoluteNoTraversal(root, "sync_root"); err != nil {
		return "", err
	}
	return path.Clean(root), nil
}

func checkAbsoluteNoTraversal(value, field string) error {
	if !strings.HasPrefix(value, "/") {
		return fmt.Errorf("%s %q is not absolute", field, value)
	}
	// Checked before cleaning, not after: path.Clean resolves "/workspace/../etc"
	// to "/etc", so a check on the cleaned form never sees the traversal it was
	// meant to catch.
	for _, segment := range strings.Split(value, "/") {
		if segment == ".." {
			return fmt.Errorf("%s %q traverses upward", field, value)
		}
	}
	return nil
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
