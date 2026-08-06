package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	agentsv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/agents/v1"
	authorizationv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/authorization/v1"
	runnerv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/runners/v1"
	terminalproxyv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/terminal_proxy/v1"
	"github.com/agynio/terminal-proxy/internal/runnerdial"
	"github.com/agynio/terminal-proxy/internal/ticket"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"nhooyr.io/websocket"
)

const (
	workloadNamePrefix = "workload-"

	// Stream identifiers prefixed to non-TTY output frames.
	streamStdout byte = 0
	streamStderr byte = 1

	// mainContainerAlias names the workload's main container without knowing
	// what it is actually called. See findExecContainer.
	mainContainerAlias = "main"

	cancelExecutionTimeout = 10 * time.Second

	terminalIdentityObjectPrefix = "identity:"
	terminalSandboxObjectPrefix  = "sandbox:"
	terminalAgentObjectPrefix    = "agent:"

	terminalSandboxConnectRelation  = "can_connect"
	terminalAgentEditConfigRelation = "can_edit_config"
)

type RunnersClient interface {
	GetWorkload(context.Context, *runnersv1.GetWorkloadRequest, ...grpc.CallOption) (*runnersv1.GetWorkloadResponse, error)
	TouchWorkload(context.Context, *runnersv1.TouchWorkloadRequest, ...grpc.CallOption) (*runnersv1.TouchWorkloadResponse, error)
}

type AuthorizationClient interface {
	Check(context.Context, *authorizationv1.CheckRequest, ...grpc.CallOption) (*authorizationv1.CheckResponse, error)
}

type AgentsClient interface {
	UpdateSandboxLastSession(context.Context, *agentsv1.UpdateSandboxLastSessionRequest, ...grpc.CallOption) (*agentsv1.UpdateSandboxLastSessionResponse, error)
}

type TicketStore interface {
	Issue(string, string, string, terminalproxyv1.SessionKind, *ticket.Command, time.Duration) (string, time.Time, error)
	Consume(string) (*ticket.Claims, error)
}

type Config struct {
	Tickets               TicketStore
	TicketTTL             time.Duration
	WebSocketURL          string
	Runners               RunnersClient
	Authorization         AuthorizationClient
	Agents                AgentsClient
	RunnerDialer          runnerdial.RunnerDialer
	TouchInterval         time.Duration
	WebSocketPingInterval time.Duration
	WebSocketPingTimeout  time.Duration
}

type Server struct {
	terminalproxyv1.UnimplementedTerminalProxyServiceServer
	tickets               TicketStore
	ticketTTL             time.Duration
	webSocketURL          string
	runners               RunnersClient
	authorization         AuthorizationClient
	agents                AgentsClient
	runnerDialer          runnerdial.RunnerDialer
	touchInterval         time.Duration
	webSocketPingInterval time.Duration
	webSocketPingTimeout  time.Duration
}

type terminalConn struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

// execState tracks what the proxy learned from the exec stream: the runner's
// execution id (needed to release the PTY) and whether the runner already ended
// the execution itself.
type execState struct {
	mu          sync.Mutex
	executionID string
	finished    bool
}

func (e *execState) setExecutionID(executionID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.executionID = strings.TrimSpace(executionID)
}

func (e *execState) markFinished() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.finished = true
}

// pendingExecutionID returns the execution id that still holds a PTY, or an
// empty string when the runner already ended the execution.
func (e *execState) pendingExecutionID() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.finished {
		return ""
	}
	return e.executionID
}

type websocketPinger interface {
	Ping(context.Context) error
}

type handshake struct {
	Cols uint32 `json:"cols"`
	Rows uint32 `json:"rows"`
}

type controlFrame struct {
	Type string `json:"type"`
	Cols uint32 `json:"cols,omitempty"`
	Rows uint32 `json:"rows,omitempty"`
}

type exitFrame struct {
	Type   string `json:"type"`
	Code   int32  `json:"code"`
	Reason string `json:"reason"`
}

func New(cfg Config) (*Server, error) {
	if cfg.Tickets == nil {
		return nil, errors.New("ticket store missing")
	}
	if cfg.TicketTTL <= 0 {
		return nil, errors.New("ticket ttl must be greater than 0")
	}
	if strings.TrimSpace(cfg.WebSocketURL) == "" {
		return nil, errors.New("websocket url missing")
	}
	if cfg.Runners == nil {
		return nil, errors.New("runners client missing")
	}
	if cfg.Authorization == nil {
		return nil, errors.New("authorization client missing")
	}
	if cfg.Agents == nil {
		return nil, errors.New("agents client missing")
	}
	if cfg.RunnerDialer == nil {
		return nil, errors.New("runner dialer missing")
	}
	if cfg.TouchInterval <= 0 {
		return nil, errors.New("touch interval must be greater than 0")
	}
	if cfg.WebSocketPingInterval <= 0 {
		return nil, errors.New("websocket ping interval must be greater than 0")
	}
	if cfg.WebSocketPingTimeout <= 0 {
		return nil, errors.New("websocket ping timeout must be greater than 0")
	}
	return &Server{
		tickets:               cfg.Tickets,
		ticketTTL:             cfg.TicketTTL,
		webSocketURL:          strings.TrimSpace(cfg.WebSocketURL),
		runners:               cfg.Runners,
		authorization:         cfg.Authorization,
		agents:                cfg.Agents,
		runnerDialer:          cfg.RunnerDialer,
		touchInterval:         cfg.TouchInterval,
		webSocketPingInterval: cfg.WebSocketPingInterval,
		webSocketPingTimeout:  cfg.WebSocketPingTimeout,
	}, nil
}

func (s *Server) IssueTicket(ctx context.Context, req *terminalproxyv1.IssueTicketRequest) (*terminalproxyv1.IssueTicketResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	identityID, workloadID, containerName, err := validateIssueTicketRequest(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	workload, err := s.loadWorkload(ctx, workloadID)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeTerminal(ctx, identityID, workload); err != nil {
		return nil, err
	}
	command, err := ticket.Resolve(req.GetKind(), req.GetCommand(), req.GetSyncRoot())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	token, expiresAt, err := s.tickets.Issue(identityID, workloadID, containerName, req.GetKind(), command, s.ticketTTL)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	expiresIn := int32(time.Until(expiresAt).Seconds())
	if expiresIn < 1 {
		expiresIn = 1
	}
	return &terminalproxyv1.IssueTicketResponse{Ticket: token, WebsocketUrl: s.webSocketURL, ExpiresInSeconds: expiresIn}, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/healthz", "/readyz":
		// The proxy holds no state and has no startup dependency: every session
		// resolves its runner on attach, so liveness and readiness coincide.
		w.WriteHeader(http.StatusOK)
	case "/terminal":
		s.serveTerminal(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) serveTerminal(w http.ResponseWriter, r *http.Request) {
	rawTicket := strings.TrimSpace(r.URL.Query().Get("ticket"))
	if rawTicket == "" {
		http.Error(w, "ticket required", http.StatusUnauthorized)
		return
	}
	claims, err := s.tickets.Consume(rawTicket)
	if err != nil {
		http.Error(w, "ticket invalid", http.StatusUnauthorized)
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	if err := s.handleSession(r.Context(), &terminalConn{conn: conn}, claims); err != nil {
		log.Printf("terminal session %s failed: %v", claims.ID, err)
	}
}

func (s *Server) handleSession(ctx context.Context, conn *terminalConn, claims *ticket.Claims) error {
	var hs handshake
	if claims.TTY {
		read, err := readHandshake(ctx, conn.conn)
		if err != nil {
			return closeWithError(ctx, conn, websocket.StatusPolicyViolation, err.Error())
		}
		hs = read
	}
	workload, err := s.loadWorkload(ctx, claims.WorkloadID)
	if err != nil {
		return closeWithError(ctx, conn, websocket.StatusInternalError, err.Error())
	}
	targetID, err := execTargetID(workload, claims.ContainerName)
	if err != nil {
		return closeWithError(ctx, conn, websocket.StatusPolicyViolation, err.Error())
	}
	runnerID := strings.TrimSpace(workload.GetRunnerId())
	if runnerID == "" {
		return closeWithError(ctx, conn, websocket.StatusInternalError, "runner id missing")
	}
	runnerClient, err := s.runnerDialer.Dial(ctx, runnerID)
	if err != nil {
		return closeWithError(ctx, conn, websocket.StatusInternalError, err.Error())
	}
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	execStream, err := runnerClient.Exec(sessionCtx)
	if err != nil {
		return closeWithError(ctx, conn, websocket.StatusInternalError, err.Error())
	}
	if err := execStream.Send(&runnerv1.ExecRequest{Msg: &runnerv1.ExecRequest_Start{Start: startRequest(claims, targetID)}}); err != nil {
		return closeWithError(ctx, conn, websocket.StatusInternalError, err.Error())
	}
	if claims.TTY {
		if err := sendResize(execStream, hs.Cols, hs.Rows); err != nil {
			return closeWithError(ctx, conn, websocket.StatusInternalError, err.Error())
		}
	}
	// SYNC sessions never report activity. A background sync session would
	// otherwise keep a sandbox running for as long as a laptop stayed
	// connected, defeating the idle timeout and the metering it bounds.
	if claims.TTY && workload.GetOwnerKind() == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
		go s.touchSandboxWorkload(sessionCtx, workload.GetMeta().GetId())
		defer s.updateSandboxLastSession(context.Background(), workload.GetOwnerId())
	}
	exec := &execState{}
	errCh := make(chan error, 3)
	go func() { errCh <- websocketToExec(sessionCtx, conn.conn, execStream, claims.TTY) }()
	go func() { errCh <- execToWebsocket(sessionCtx, conn, execStream, exec, claims.TTY) }()
	go func() { errCh <- websocketLiveness(sessionCtx, conn, s.webSocketPingInterval, s.webSocketPingTimeout) }()
	err = <-errCh
	cancel()
	// The session ended before the runner reported an exit — the client is gone
	// (missed pongs, dropped socket, write failure) and the PTY is still held.
	s.cancelExecution(runnerClient, exec.pendingExecutionID())
	_ = conn.close(websocket.StatusNormalClosure, "terminal session ended")
	if err == nil || errors.Is(err, context.Canceled) || websocket.CloseStatus(err) == websocket.StatusNormalClosure {
		return nil
	}
	return err
}

// loadWorkload resolves the hosting runner for a workload. The call is an
// internal, identity-less Runners lookup: terminal access is authorized by
// authorizeTerminal, not by the Runners service.
func (s *Server) loadWorkload(ctx context.Context, workloadID string) (*runnersv1.Workload, error) {
	resp, err := s.runners.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: workloadID})
	if err != nil {
		return nil, err
	}
	workload := resp.GetWorkload()
	if workload == nil {
		return nil, status.Error(codes.NotFound, "workload not found")
	}
	return workload, nil
}

func validateIssueTicketRequest(req *terminalproxyv1.IssueTicketRequest) (string, string, string, error) {
	identityID, err := parseUUID(req.GetIdentityId(), "identity_id")
	if err != nil {
		return "", "", "", err
	}
	workloadID, err := parseUUID(req.GetWorkloadId(), "workload_id")
	if err != nil {
		return "", "", "", err
	}
	containerName := strings.TrimSpace(req.GetContainerName())
	if containerName == "" {
		return "", "", "", errors.New("container_name is required")
	}
	return identityID, workloadID, containerName, nil
}

func parseUUID(value, field string) (string, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(value))
	if err != nil {
		return "", fmt.Errorf("%s must be a UUID", field)
	}
	return parsed.String(), nil
}

func (s *Server) authorizeTerminal(ctx context.Context, identityID string, workload *runnersv1.Workload) error {
	tuple, err := terminalAuthorizationTuple(identityID, workload)
	if err != nil {
		return err
	}
	resp, err := s.authorization.Check(ctx, &authorizationv1.CheckRequest{TupleKey: tuple})
	if err != nil {
		return err
	}
	if resp == nil || !resp.GetAllowed() {
		return status.Error(codes.PermissionDenied, "terminal access denied")
	}
	return nil
}

func terminalAuthorizationTuple(identityID string, workload *runnersv1.Workload) (*authorizationv1.TupleKey, error) {
	if workload == nil || workload.GetMeta().GetId() == "" {
		return nil, status.Error(codes.FailedPrecondition, "workload missing")
	}
	user := terminalIdentityObjectPrefix + strings.TrimSpace(identityID)
	switch workload.GetOwnerKind() {
	case runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX:
		ownerID, err := parseUUID(workload.GetOwnerId(), "owner_id")
		if err != nil {
			return nil, status.Error(codes.PermissionDenied, "terminal access denied")
		}
		return &authorizationv1.TupleKey{User: user, Relation: terminalSandboxConnectRelation, Object: terminalSandboxObjectPrefix + ownerID}, nil
	case runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE:
		agentID, err := parseUUID(workload.GetAgentClassId(), "agent_class_id")
		if err != nil {
			return nil, status.Error(codes.PermissionDenied, "terminal access denied")
		}
		return &authorizationv1.TupleKey{User: user, Relation: terminalAgentEditConfigRelation, Object: terminalAgentObjectPrefix + agentID}, nil
	case runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_UNSPECIFIED:
		return nil, status.Error(codes.PermissionDenied, "terminal access denied")
	default:
		return nil, status.Error(codes.PermissionDenied, "terminal access denied")
	}
}

func (s *Server) touchSandboxWorkload(ctx context.Context, workloadID string) {
	s.touchWorkload(ctx, workloadID)
	ticker := time.NewTicker(s.touchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.touchWorkload(ctx, workloadID)
		}
	}
}

func (s *Server) touchWorkload(ctx context.Context, workloadID string) {
	if strings.TrimSpace(workloadID) == "" {
		return
	}
	if _, err := s.runners.TouchWorkload(ctx, &runnersv1.TouchWorkloadRequest{Id: workloadID}); err != nil && ctx.Err() == nil {
		log.Printf("touch workload %s: %v", workloadID, err)
	}
}

// cancelExecution releases the PTY the runner still holds for a session that
// ended abnormally. The session context is already cancelled by this point, so
// the call runs on its own bounded context.
func (s *Server) cancelExecution(runnerClient runnerdial.RunnerClient, executionID string) {
	if strings.TrimSpace(executionID) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), cancelExecutionTimeout)
	defer cancel()
	if _, err := runnerClient.CancelExecution(ctx, &runnerv1.CancelExecutionRequest{ExecutionId: executionID}); err != nil {
		log.Printf("cancel execution %s: %v", executionID, err)
	}
}

func (s *Server) updateSandboxLastSession(ctx context.Context, sandboxID string) {
	if strings.TrimSpace(sandboxID) == "" {
		return
	}
	_, err := s.agents.UpdateSandboxLastSession(ctx, &agentsv1.UpdateSandboxLastSessionRequest{
		Id:            sandboxID,
		LastSessionAt: timestamppb.New(time.Now().UTC()),
	})
	if err != nil {
		log.Printf("update sandbox last session %s: %v", sandboxID, err)
	}
}

func readHandshake(ctx context.Context, conn *websocket.Conn) (handshake, error) {
	messageType, data, err := conn.Read(ctx)
	if err != nil {
		return handshake{}, err
	}
	if messageType != websocket.MessageText {
		return handshake{}, errors.New("handshake must be a JSON text frame")
	}
	var hs handshake
	if err := json.Unmarshal(data, &hs); err != nil {
		return handshake{}, errors.New("handshake JSON invalid")
	}
	if hs.Cols == 0 || hs.Rows == 0 {
		return handshake{}, errors.New("handshake cols and rows are required")
	}
	return hs, nil
}

func startRequest(claims *ticket.Claims, targetID string) *runnerv1.ExecStartRequest {
	start := &runnerv1.ExecStartRequest{
		RequestId: claims.ID,
		TargetId:  targetID,
		// A non-TTY session needs the streams kept apart: one byte of stderr
		// inside the protocol stream corrupts it beyond recovery.
		Options: &runnerv1.ExecOptions{Tty: claims.TTY, SeparateStderr: !claims.TTY},
	}
	if len(claims.CommandArgv) > 0 {
		start.CommandArgv = append([]string(nil), claims.CommandArgv...)
	} else {
		start.CommandShell = claims.CommandShell
	}
	return start
}

func execTargetID(workload *runnersv1.Workload, containerName string) (string, error) {
	containerName = strings.TrimSpace(containerName)
	if containerName == "" {
		return "", errors.New("container name missing")
	}
	container, err := findExecContainer(workload, containerName)
	if err != nil {
		return "", err
	}
	if container.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_MAIN {
		return "", fmt.Errorf("container %s is not supported by current runner exec", containerName)
	}
	return workloadExecTargetID(workload)
}

// findExecContainer matches on the literal container name first, so a client
// that lists a workload's containers (the Console) always gets the one it
// picked. mainContainerAlias exists for the client that cannot: a sandbox's
// main container is named by the orchestrator after the sandbox id
// (sandbox-<prefix>), a name the CLI has no way to know and no business
// reconstructing, while "the sandbox's shell" is unambiguous — a workload has
// exactly one main container.
func findExecContainer(workload *runnersv1.Workload, containerName string) (*runnersv1.Container, error) {
	for _, container := range workload.GetContainers() {
		if container.GetName() == containerName {
			return container, nil
		}
	}
	if containerName == mainContainerAlias {
		for _, container := range workload.GetContainers() {
			if container.GetRole() == runnersv1.ContainerRole_CONTAINER_ROLE_MAIN {
				return container, nil
			}
		}
	}
	return nil, fmt.Errorf("container %s not found", containerName)
}

func workloadExecTargetID(workload *runnersv1.Workload) (string, error) {
	instanceID := strings.TrimSpace(workload.GetInstanceId())
	if instanceID != "" {
		return runnerPodTargetID(instanceID), nil
	}
	workloadID := strings.TrimSpace(workload.GetMeta().GetId())
	if workloadID == "" {
		return "", errors.New("workload id missing")
	}
	return runnerPodTargetID(workloadID), nil
}

func runnerPodTargetID(id string) string {
	id = strings.TrimSpace(id)
	if strings.HasPrefix(id, workloadNamePrefix) {
		return id
	}
	return workloadNamePrefix + id
}

func websocketToExec(ctx context.Context, conn *websocket.Conn, execStream runnerv1.RunnerService_ExecClient, tty bool) error {
	for {
		messageType, data, err := conn.Read(ctx)
		if err != nil {
			_ = execStream.CloseSend()
			return err
		}
		switch messageType {
		case websocket.MessageBinary:
			if err := execStream.Send(&runnerv1.ExecRequest{Msg: &runnerv1.ExecRequest_Stdin{Stdin: &runnerv1.ExecStdin{Data: data}}}); err != nil {
				return err
			}
		case websocket.MessageText:
			var frame controlFrame
			if err := json.Unmarshal(data, &frame); err != nil {
				return err
			}
			if frame.Type != "resize" {
				return fmt.Errorf("unsupported control frame %q", frame.Type)
			}
			if !tty {
				return errors.New("resize is meaningful only to TTY sessions")
			}
			if err := sendResize(execStream, frame.Cols, frame.Rows); err != nil {
				return err
			}
		}
	}
}

func execToWebsocket(ctx context.Context, conn *terminalConn, execStream runnerv1.RunnerService_ExecClient, exec *execState, tty bool) error {
	for {
		resp, err := execStream.Recv()
		if errors.Is(err, io.EOF) {
			exec.markFinished()
			return conn.close(websocket.StatusNormalClosure, "exec complete")
		}
		if err != nil {
			_ = writeExit(ctx, conn, 1, "error")
			return err
		}
		switch event := resp.GetEvent().(type) {
		case *runnerv1.ExecResponse_Started:
			exec.setExecutionID(event.Started.GetExecutionId())
		case *runnerv1.ExecResponse_Stdout:
			if err := conn.write(ctx, websocket.MessageBinary, tagged(tty, streamStdout, event.Stdout.GetData())); err != nil {
				return err
			}
		case *runnerv1.ExecResponse_Stderr:
			if err := conn.write(ctx, websocket.MessageBinary, tagged(tty, streamStderr, event.Stderr.GetData())); err != nil {
				return err
			}
		case *runnerv1.ExecResponse_Exit:
			exec.markFinished()
			reason := exitReason(event.Exit.GetReason())
			if err := writeExit(ctx, conn, event.Exit.GetExitCode(), reason); err != nil {
				return err
			}
			return nil
		case *runnerv1.ExecResponse_Error:
			exec.markFinished()
			_ = writeExit(ctx, conn, 1, "error")
			return fmt.Errorf("runner exec error %s: %s", event.Error.GetCode(), event.Error.GetMessage())
		}
	}
}

// tagged prefixes non-TTY output with the stream it came from. TTY output is
// left untouched: a PTY merges the two at the kernel anyway, and every existing
// client reads those frames as raw bytes.
func tagged(tty bool, stream byte, data []byte) []byte {
	if tty {
		return data
	}
	framed := make([]byte, 0, len(data)+1)
	framed = append(framed, stream)
	return append(framed, data...)
}

func websocketLiveness(ctx context.Context, pinger websocketPinger, interval, timeout time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, timeout)
			err := pinger.Ping(pingCtx)
			cancel()
			if err != nil {
				return fmt.Errorf("websocket ping: %w", err)
			}
		}
	}
}

func (c *terminalConn) Ping(ctx context.Context) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.Ping(ctx)
}

func (c *terminalConn) write(ctx context.Context, messageType websocket.MessageType, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.Write(ctx, messageType, data)
}

func (c *terminalConn) close(code websocket.StatusCode, reason string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.Close(code, reason)
}

func sendResize(execStream runnerv1.RunnerService_ExecClient, cols, rows uint32) error {
	if cols == 0 || rows == 0 {
		return errors.New("resize cols and rows are required")
	}
	return execStream.Send(&runnerv1.ExecRequest{Msg: &runnerv1.ExecRequest_Resize{Resize: &runnerv1.ExecResize{Cols: cols, Rows: rows}}})
}

func writeExit(ctx context.Context, conn *terminalConn, code int32, reason string) error {
	payload, err := json.Marshal(exitFrame{Type: "exit", Code: code, Reason: reason})
	if err != nil {
		return err
	}
	return conn.write(ctx, websocket.MessageText, payload)
}

func exitReason(reason runnerv1.ExecExitReason) string {
	switch reason {
	case runnerv1.ExecExitReason_EXEC_EXIT_REASON_COMPLETED:
		return "completed"
	case runnerv1.ExecExitReason_EXEC_EXIT_REASON_CANCELLED:
		return "cancelled"
	case runnerv1.ExecExitReason_EXEC_EXIT_REASON_TIMEOUT, runnerv1.ExecExitReason_EXEC_EXIT_REASON_IDLE_TIMEOUT, runnerv1.ExecExitReason_EXEC_EXIT_REASON_RUNNER_ERROR:
		return "error"
	case runnerv1.ExecExitReason_EXEC_EXIT_REASON_UNSPECIFIED:
		return "error"
	default:
		return "error"
	}
}

func closeWithError(ctx context.Context, conn *terminalConn, code websocket.StatusCode, reason string) error {
	_ = writeExit(ctx, conn, 1, "error")
	_ = conn.close(code, reason)
	return errors.New(reason)
}

func ValidateUUID(value string) error {
	_, err := uuid.Parse(strings.TrimSpace(value))
	return err
}
