package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
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
	"nhooyr.io/websocket"
)

func TestIssueTicketReturnsSignedShortLivedTicket(t *testing.T) {
	store := ticket.NewStore([]byte("01234567890123456789012345678901"))
	identityID := uuid.NewString()
	workloadID := uuid.NewString()
	sandboxID := uuid.NewString()
	authorization := &fakeAuthorizationClient{allowed: true}
	server := newTestServer(t, store, &fakeRunnersClient{workload: sandboxWorkload(workloadID, sandboxID)}, authorization, &fakeAgentsClient{}, &fakeRunnerDialer{})
	resp, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{IdentityId: identityID, WorkloadId: workloadID, ContainerName: "main"})
	if err != nil {
		t.Fatalf("issue ticket: %v", err)
	}
	assertAuthorizationTuple(t, authorization.lastRequest(), "identity:"+identityID, "can_connect", "sandbox:"+sandboxID)
	if resp.GetTicket() == "" {
		t.Fatal("expected ticket")
	}
	if resp.GetWebsocketUrl() != "ws://proxy/terminal" {
		t.Fatalf("unexpected websocket url %q", resp.GetWebsocketUrl())
	}
	if resp.GetExpiresInSeconds() < 1 || resp.GetExpiresInSeconds() > 30 {
		t.Fatalf("unexpected expires_in_seconds %d", resp.GetExpiresInSeconds())
	}
	claims, err := store.Consume(resp.GetTicket())
	if err != nil {
		t.Fatalf("consume ticket: %v", err)
	}
	if claims.IdentityID != identityID || claims.WorkloadID != workloadID || claims.ContainerName != "main" {
		t.Fatalf("unexpected claims: %#v", claims)
	}
	if _, err := store.Consume(resp.GetTicket()); err == nil {
		t.Fatal("expected single-use ticket")
	}
}

func TestWebSocketBridgesRunnerExecAndSandboxActivity(t *testing.T) {
	store := ticket.NewStore([]byte("01234567890123456789012345678901"))
	workloadID := uuid.NewString()
	runnerID := uuid.NewString()
	sandboxID := uuid.NewString()
	runner := &fakeRunnerClient{}
	runners := &fakeRunnersClient{workload: &runnersv1.Workload{
		Meta:       &runnersv1.EntityMeta{Id: workloadID},
		RunnerId:   runnerID,
		Status:     runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		OwnerKind:  runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:    sandboxID,
		InstanceId: stringPtr("workload-" + workloadID),
		Containers: []*runnersv1.Container{
			{Name: "main", ContainerId: "container-runtime-id", Role: runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, Status: runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING},
		},
	}}
	agents := &fakeAgentsClient{}
	server := newTestServer(t, store, runners, &fakeAuthorizationClient{allowed: true}, agents, &fakeRunnerDialer{runner: runner})
	issued, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{
		IdentityId:    uuid.NewString(),
		WorkloadId:    workloadID,
		ContainerName: "main",
		Command:       &terminalproxyv1.TerminalCommand{Command: &terminalproxyv1.TerminalCommand_Shell{Shell: "echo ready"}},
	})
	if err != nil {
		t.Fatalf("issue ticket: %v", err)
	}
	conn := dialTestTerminal(t, server, issued.GetTicket())
	defer conn.CloseNow()
	writeHandshake(t, conn, 120, 40)
	start := runner.recv(t)
	if start.GetStart() == nil {
		t.Fatalf("expected start, got %v", start)
	}
	if start.GetStart().GetTargetId() != "workload-"+workloadID {
		t.Fatalf("unexpected target id %q", start.GetStart().GetTargetId())
	}
	if start.GetStart().GetCommandShell() != "echo ready" {
		t.Fatalf("unexpected command %q", start.GetStart().GetCommandShell())
	}
	resize := runner.recv(t)
	if resize.GetResize().GetCols() != 120 || resize.GetResize().GetRows() != 40 {
		t.Fatalf("unexpected initial resize: %v", resize.GetResize())
	}
	if err := conn.Write(context.Background(), websocket.MessageBinary, []byte("abc")); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	stdin := runner.recv(t)
	if string(stdin.GetStdin().GetData()) != "abc" {
		t.Fatalf("unexpected stdin: %v", stdin.GetStdin())
	}
	if err := runner.send(&runnerv1.ExecResponse{Event: &runnerv1.ExecResponse_Stdout{Stdout: &runnerv1.ExecOutput{Data: []byte("out")}}}); err != nil {
		t.Fatalf("send stdout: %v", err)
	}
	messageType, data, err := conn.Read(context.Background())
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if messageType != websocket.MessageBinary || string(data) != "out" {
		t.Fatalf("unexpected websocket output type=%v data=%q", messageType, data)
	}
	if err := runner.send(&runnerv1.ExecResponse{Event: &runnerv1.ExecResponse_Exit{Exit: &runnerv1.ExecExit{ExitCode: 7, Reason: runnerv1.ExecExitReason_EXEC_EXIT_REASON_COMPLETED}}}); err != nil {
		t.Fatalf("send exit: %v", err)
	}
	messageType, data, err = conn.Read(context.Background())
	if err != nil {
		t.Fatalf("read exit: %v", err)
	}
	if messageType != websocket.MessageText {
		t.Fatalf("expected text exit frame")
	}
	var exit exitFrame
	if err := json.Unmarshal(data, &exit); err != nil {
		t.Fatalf("parse exit frame: %v", err)
	}
	if exit.Type != "exit" || exit.Code != 7 || exit.Reason != "completed" {
		t.Fatalf("unexpected exit frame: %#v", exit)
	}
	eventually(t, func() bool { return runners.touchCount() >= 1 })
	eventually(t, func() bool { return agents.lastSessionSandboxID() == sandboxID })
	time.Sleep(20 * time.Millisecond)
	if cancelled := runner.cancelledExecutions(); len(cancelled) != 0 {
		t.Fatalf("expected no cancel on normal shell exit, got %v", cancelled)
	}
}

func TestSessionResolvesRunnerWithGetWorkload(t *testing.T) {
	store := ticket.NewStore([]byte("01234567890123456789012345678901"))
	workloadID := uuid.NewString()
	runners := &fakeRunnersClient{workload: sandboxWorkload(workloadID, uuid.NewString())}
	server := newTestServer(t, store, runners, &fakeAuthorizationClient{allowed: true}, &fakeAgentsClient{}, &fakeRunnerDialer{})

	if _, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{IdentityId: uuid.NewString(), WorkloadId: workloadID, ContainerName: "main"}); err != nil {
		t.Fatalf("issue ticket: %v", err)
	}
	gets := runners.getIDs()
	if len(gets) != 1 || gets[0] != workloadID {
		t.Fatalf("expected a single GetWorkload for %s, got %v", workloadID, gets)
	}
}

func TestIssueTicketPropagatesWorkloadLookupError(t *testing.T) {
	store := ticket.NewStore([]byte("01234567890123456789012345678901"))
	runners := &fakeRunnersClient{getErr: status.Error(codes.NotFound, "workload not found")}
	server := newTestServer(t, store, runners, &fakeAuthorizationClient{allowed: true}, &fakeAgentsClient{}, &fakeRunnerDialer{})

	_, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{IdentityId: uuid.NewString(), WorkloadId: uuid.NewString(), ContainerName: "main"})
	assertCode(t, err, codes.NotFound)
}

func TestClientDisconnectCancelsExecution(t *testing.T) {
	store := ticket.NewStore([]byte("01234567890123456789012345678901"))
	workloadID := uuid.NewString()
	runner := &fakeRunnerClient{}
	runners := &fakeRunnersClient{workload: &runnersv1.Workload{
		Meta:         &runnersv1.EntityMeta{Id: workloadID},
		RunnerId:     uuid.NewString(),
		Status:       runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		OwnerKind:    runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:      uuid.NewString(),
		AgentClassId: stringPtr(uuid.NewString()),
		Containers:   []*runnersv1.Container{{Name: "main", Role: runnersv1.ContainerRole_CONTAINER_ROLE_MAIN}},
	}}
	server := newTestServer(t, store, runners, &fakeAuthorizationClient{allowed: true}, &fakeAgentsClient{}, &fakeRunnerDialer{runner: runner})
	issued, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{IdentityId: uuid.NewString(), WorkloadId: workloadID, ContainerName: "main"})
	if err != nil {
		t.Fatalf("issue ticket: %v", err)
	}
	conn := dialTestTerminal(t, server, issued.GetTicket())
	writeHandshake(t, conn, 80, 24)
	_ = runner.recv(t)
	_ = runner.recv(t)
	if err := runner.send(&runnerv1.ExecResponse{Event: &runnerv1.ExecResponse_Started{Started: &runnerv1.ExecStarted{ExecutionId: "execution-1"}}}); err != nil {
		t.Fatalf("send started: %v", err)
	}
	// Reading output ordered after ExecStarted proves the proxy recorded the
	// execution id before the client goes away.
	if err := runner.send(&runnerv1.ExecResponse{Event: &runnerv1.ExecResponse_Stdout{Stdout: &runnerv1.ExecOutput{Data: []byte("out")}}}); err != nil {
		t.Fatalf("send stdout: %v", err)
	}
	if _, _, err := conn.Read(context.Background()); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	conn.CloseNow()

	eventually(t, func() bool {
		cancelled := runner.cancelledExecutions()
		return len(cancelled) == 1 && cancelled[0] == "execution-1"
	})
}

func TestHealthEndpointsRespondOK(t *testing.T) {
	store := ticket.NewStore([]byte("01234567890123456789012345678901"))
	server := newTestServer(t, store, &fakeRunnersClient{}, &fakeAuthorizationClient{allowed: true}, &fakeAgentsClient{}, &fakeRunnerDialer{})
	for _, path := range []string{"/healthz", "/readyz"} {
		recorder := httptest.NewRecorder()
		server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want %d", path, recorder.Code, http.StatusOK)
		}
	}
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("/nope status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func TestExecTargetUsesWorkloadPodAndRejectsUnsupportedContainer(t *testing.T) {
	workloadID := uuid.NewString()
	workload := &runnersv1.Workload{
		Meta: &runnersv1.EntityMeta{Id: workloadID},
		Containers: []*runnersv1.Container{
			{Name: "main", ContainerId: "runtime-main", Role: runnersv1.ContainerRole_CONTAINER_ROLE_MAIN},
			{Name: "sidecar", ContainerId: "runtime-sidecar", Role: runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR},
		},
	}
	targetID, err := execTargetID(workload, "main")
	if err != nil {
		t.Fatalf("exec target: %v", err)
	}
	if targetID != "workload-"+workloadID {
		t.Fatalf("expected pod target, got %q", targetID)
	}
	if _, err := execTargetID(workload, "sidecar"); err == nil {
		t.Fatal("expected sidecar target to be rejected until Runner.Exec supports container selection")
	}
}

func TestWebSocketLivenessStopsOnMissedPong(t *testing.T) {
	pinger := &fakePinger{err: errors.New("pong timeout")}
	err := websocketLiveness(context.Background(), pinger, time.Millisecond, time.Millisecond)
	if err == nil {
		t.Fatal("expected liveness error")
	}
	if pinger.count() != 1 {
		t.Fatalf("expected one ping, got %d", pinger.count())
	}
}

func TestIssueTicketAuthorizesSandboxWorkload(t *testing.T) {
	store := &countingTicketStore{store: ticket.NewStore([]byte("01234567890123456789012345678901"))}
	identityID := uuid.NewString()
	workloadID := uuid.NewString()
	sandboxID := uuid.NewString()
	authorization := &fakeAuthorizationClient{allowed: true}
	server := newTestServer(t, store, &fakeRunnersClient{workload: sandboxWorkload(workloadID, sandboxID)}, authorization, &fakeAgentsClient{}, &fakeRunnerDialer{})

	resp, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{IdentityId: identityID, WorkloadId: workloadID, ContainerName: "main"})
	if err != nil {
		t.Fatalf("issue ticket: %v", err)
	}
	if resp.GetTicket() == "" {
		t.Fatal("expected ticket")
	}
	if store.issueCount() != 1 {
		t.Fatalf("expected one ticket issue, got %d", store.issueCount())
	}
	assertAuthorizationTuple(t, authorization.lastRequest(), "identity:"+identityID, "can_connect", "sandbox:"+sandboxID)
}

func TestIssueTicketDeniesSandboxWorkload(t *testing.T) {
	store := &countingTicketStore{store: ticket.NewStore([]byte("01234567890123456789012345678901"))}
	workloadID := uuid.NewString()
	server := newTestServer(t, store, &fakeRunnersClient{workload: sandboxWorkload(workloadID, uuid.NewString())}, &fakeAuthorizationClient{}, &fakeAgentsClient{}, &fakeRunnerDialer{})

	_, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{IdentityId: uuid.NewString(), WorkloadId: workloadID, ContainerName: "main"})
	assertCode(t, err, codes.PermissionDenied)
	if store.issueCount() != 0 {
		t.Fatalf("expected no ticket before authz, got %d", store.issueCount())
	}
}

func TestIssueTicketAuthorizesAgentWorkload(t *testing.T) {
	store := &countingTicketStore{store: ticket.NewStore([]byte("01234567890123456789012345678901"))}
	identityID := uuid.NewString()
	workloadID := uuid.NewString()
	agentID := uuid.NewString()
	authorization := &fakeAuthorizationClient{allowed: true}
	server := newTestServer(t, store, &fakeRunnersClient{workload: agentWorkload(workloadID, agentID)}, authorization, &fakeAgentsClient{}, &fakeRunnerDialer{})

	resp, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{IdentityId: identityID, WorkloadId: workloadID, ContainerName: "main"})
	if err != nil {
		t.Fatalf("issue ticket: %v", err)
	}
	if resp.GetTicket() == "" {
		t.Fatal("expected ticket")
	}
	assertAuthorizationTuple(t, authorization.lastRequest(), "identity:"+identityID, "can_edit_config", "agent:"+agentID)
}

func TestIssueTicketDeniesAgentWorkload(t *testing.T) {
	store := &countingTicketStore{store: ticket.NewStore([]byte("01234567890123456789012345678901"))}
	workloadID := uuid.NewString()
	server := newTestServer(t, store, &fakeRunnersClient{workload: agentWorkload(workloadID, uuid.NewString())}, &fakeAuthorizationClient{}, &fakeAgentsClient{}, &fakeRunnerDialer{})

	_, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{IdentityId: uuid.NewString(), WorkloadId: workloadID, ContainerName: "main"})
	assertCode(t, err, codes.PermissionDenied)
	if store.issueCount() != 0 {
		t.Fatalf("expected no ticket before authz, got %d", store.issueCount())
	}
}

func TestIssueTicketDeniesUnsupportedOwnerKind(t *testing.T) {
	store := &countingTicketStore{store: ticket.NewStore([]byte("01234567890123456789012345678901"))}
	workloadID := uuid.NewString()
	authorization := &fakeAuthorizationClient{allowed: true}
	server := newTestServer(t, store, &fakeRunnersClient{workload: &runnersv1.Workload{
		Meta:      &runnersv1.EntityMeta{Id: workloadID},
		OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_UNSPECIFIED,
	}}, authorization, &fakeAgentsClient{}, &fakeRunnerDialer{})

	_, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{IdentityId: uuid.NewString(), WorkloadId: workloadID, ContainerName: "main"})
	assertCode(t, err, codes.PermissionDenied)
	if authorization.checkCount() != 0 {
		t.Fatalf("expected no authorization call, got %d", authorization.checkCount())
	}
	if store.issueCount() != 0 {
		t.Fatalf("expected no ticket before authz, got %d", store.issueCount())
	}
}

func TestIssueTicketDeniesMalformedOwner(t *testing.T) {
	store := &countingTicketStore{store: ticket.NewStore([]byte("01234567890123456789012345678901"))}
	workloadID := uuid.NewString()
	authorization := &fakeAuthorizationClient{allowed: true}
	server := newTestServer(t, store, &fakeRunnersClient{workload: &runnersv1.Workload{
		Meta:      &runnersv1.EntityMeta{Id: workloadID},
		OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:   "not-a-uuid",
	}}, authorization, &fakeAgentsClient{}, &fakeRunnerDialer{})

	_, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{IdentityId: uuid.NewString(), WorkloadId: workloadID, ContainerName: "main"})
	assertCode(t, err, codes.PermissionDenied)
	if authorization.checkCount() != 0 {
		t.Fatalf("expected no authorization call, got %d", authorization.checkCount())
	}
	if store.issueCount() != 0 {
		t.Fatalf("expected no ticket before authz, got %d", store.issueCount())
	}
}

func TestIssueTicketPropagatesAuthorizationError(t *testing.T) {
	store := &countingTicketStore{store: ticket.NewStore([]byte("01234567890123456789012345678901"))}
	workloadID := uuid.NewString()
	server := newTestServer(t, store, &fakeRunnersClient{workload: sandboxWorkload(workloadID, uuid.NewString())}, &fakeAuthorizationClient{err: status.Error(codes.Unavailable, "authz unavailable")}, &fakeAgentsClient{}, &fakeRunnerDialer{})

	_, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{IdentityId: uuid.NewString(), WorkloadId: workloadID, ContainerName: "main"})
	assertCode(t, err, codes.Unavailable)
	if store.issueCount() != 0 {
		t.Fatalf("expected no ticket before authz, got %d", store.issueCount())
	}
}

func TestIssueTicketDoesNotAuthorizeOrIssueForInvalidRequest(t *testing.T) {
	store := &countingTicketStore{store: ticket.NewStore([]byte("01234567890123456789012345678901"))}
	authorization := &fakeAuthorizationClient{allowed: true}
	server := newTestServer(t, store, &fakeRunnersClient{workload: sandboxWorkload(uuid.NewString(), uuid.NewString())}, authorization, &fakeAgentsClient{}, &fakeRunnerDialer{})

	_, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{IdentityId: "not-a-uuid", WorkloadId: uuid.NewString(), ContainerName: "main"})
	assertCode(t, err, codes.InvalidArgument)
	if authorization.checkCount() != 0 {
		t.Fatalf("expected no authorization call, got %d", authorization.checkCount())
	}
	if store.issueCount() != 0 {
		t.Fatalf("expected no ticket before authz, got %d", store.issueCount())
	}
}

func TestAgentWorkloadDoesNotTouchOrUpdateSandbox(t *testing.T) {
	store := ticket.NewStore([]byte("01234567890123456789012345678901"))
	workloadID := uuid.NewString()
	runner := &fakeRunnerClient{}
	runners := &fakeRunnersClient{workload: &runnersv1.Workload{
		Meta:         &runnersv1.EntityMeta{Id: workloadID},
		RunnerId:     uuid.NewString(),
		Status:       runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		OwnerKind:    runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:      uuid.NewString(),
		AgentClassId: stringPtr(uuid.NewString()),
		Containers:   []*runnersv1.Container{{Name: "main", Role: runnersv1.ContainerRole_CONTAINER_ROLE_MAIN}},
	}}
	agents := &fakeAgentsClient{}
	server := newTestServer(t, store, runners, &fakeAuthorizationClient{allowed: true}, agents, &fakeRunnerDialer{runner: runner})
	issued, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{IdentityId: uuid.NewString(), WorkloadId: workloadID, ContainerName: "main"})
	if err != nil {
		t.Fatalf("issue ticket: %v", err)
	}
	conn := dialTestTerminal(t, server, issued.GetTicket())
	defer conn.CloseNow()
	writeHandshake(t, conn, 80, 24)
	_ = runner.recv(t)
	_ = runner.recv(t)
	if err := runner.send(&runnerv1.ExecResponse{Event: &runnerv1.ExecResponse_Exit{Exit: &runnerv1.ExecExit{Reason: runnerv1.ExecExitReason_EXEC_EXIT_REASON_COMPLETED}}}); err != nil {
		t.Fatalf("send exit: %v", err)
	}
	_, _, _ = conn.Read(context.Background())
	time.Sleep(20 * time.Millisecond)
	if runners.touchCount() != 0 {
		t.Fatalf("expected no touches, got %d", runners.touchCount())
	}
	if agents.lastSessionSandboxID() != "" {
		t.Fatalf("expected no sandbox update, got %q", agents.lastSessionSandboxID())
	}
}

func newTestServer(t *testing.T, store TicketStore, runners *fakeRunnersClient, authorization *fakeAuthorizationClient, agents *fakeAgentsClient, dialer *fakeRunnerDialer) *Server {
	t.Helper()
	server, err := New(Config{
		Tickets:               store,
		TicketTTL:             30 * time.Second,
		WebSocketURL:          "ws://proxy/terminal",
		Runners:               runners,
		Authorization:         authorization,
		Agents:                agents,
		RunnerDialer:          dialer,
		TouchInterval:         time.Hour,
		WebSocketPingInterval: time.Hour,
		WebSocketPingTimeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return server
}

func dialTestTerminal(t *testing.T, server *Server, ticket string) *websocket.Conn {
	t.Helper()
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	conn, _, err := websocket.Dial(context.Background(), "ws"+httpServer.URL[len("http"):]+"/terminal?ticket="+ticket, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	return conn
}

func writeHandshake(t *testing.T, conn *websocket.Conn, cols, rows uint32) {
	t.Helper()
	payload, err := json.Marshal(handshake{Cols: cols, Rows: rows})
	if err != nil {
		t.Fatalf("marshal handshake: %v", err)
	}
	if err := conn.Write(context.Background(), websocket.MessageText, payload); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
}

type fakeRunnersClient struct {
	workload *runnersv1.Workload
	getErr   error
	mu       sync.Mutex
	gets     []string
	touches  []string
}

func (f *fakeRunnersClient) GetWorkload(_ context.Context, req *runnersv1.GetWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.GetWorkloadResponse, error) {
	f.mu.Lock()
	f.gets = append(f.gets, req.GetId())
	f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	if f.workload == nil || f.workload.GetMeta().GetId() != req.GetId() {
		return nil, status.Error(codes.NotFound, "workload not found")
	}
	return &runnersv1.GetWorkloadResponse{Workload: f.workload}, nil
}

func (f *fakeRunnersClient) getIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.gets...)
}

func (f *fakeRunnersClient) TouchWorkload(_ context.Context, req *runnersv1.TouchWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.TouchWorkloadResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.touches = append(f.touches, req.GetId())
	return &runnersv1.TouchWorkloadResponse{}, nil
}

func (f *fakeRunnersClient) touchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.touches)
}

type fakeAuthorizationClient struct {
	allowed bool
	err     error
	mu      sync.Mutex
	checks  []*authorizationv1.CheckRequest
}

func (f *fakeAuthorizationClient) Check(_ context.Context, req *authorizationv1.CheckRequest, _ ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
	f.mu.Lock()
	f.checks = append(f.checks, req)
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return &authorizationv1.CheckResponse{Allowed: f.allowed}, nil
}

func (f *fakeAuthorizationClient) lastRequest() *authorizationv1.CheckRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.checks) == 0 {
		return nil
	}
	return f.checks[len(f.checks)-1]
}

func (f *fakeAuthorizationClient) checkCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.checks)
}

type countingTicketStore struct {
	store *ticket.Store
	mu    sync.Mutex
	count int
}

func (s *countingTicketStore) Issue(identityID, workloadID, containerName string, command *terminalproxyv1.TerminalCommand, ttl time.Duration) (string, time.Time, error) {
	s.mu.Lock()
	s.count++
	s.mu.Unlock()
	return s.store.Issue(identityID, workloadID, containerName, command, ttl)
}

func (s *countingTicketStore) Consume(raw string) (*ticket.Claims, error) {
	return s.store.Consume(raw)
}

func (s *countingTicketStore) issueCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

type fakeAgentsClient struct {
	mu        sync.Mutex
	sandboxID string
}

func (f *fakeAgentsClient) UpdateSandboxLastSession(_ context.Context, req *agentsv1.UpdateSandboxLastSessionRequest, _ ...grpc.CallOption) (*agentsv1.UpdateSandboxLastSessionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sandboxID = req.GetId()
	return &agentsv1.UpdateSandboxLastSessionResponse{}, nil
}

func (f *fakeAgentsClient) lastSessionSandboxID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sandboxID
}

type fakeRunnerDialer struct {
	runner *fakeRunnerClient
}

func (f *fakeRunnerDialer) Dial(context.Context, string) (runnerdial.RunnerClient, error) {
	return f.runner, nil
}

func (f *fakeRunnerDialer) Close() {}

type fakeRunnerClient struct {
	once      sync.Once
	in        chan *runnerv1.ExecRequest
	out       chan *runnerv1.ExecResponse
	mu        sync.Mutex
	ctx       context.Context
	cancelled []string
}

func (f *fakeRunnerClient) CancelExecution(_ context.Context, req *runnerv1.CancelExecutionRequest, _ ...grpc.CallOption) (*runnerv1.CancelExecutionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled = append(f.cancelled, req.GetExecutionId())
	return &runnerv1.CancelExecutionResponse{}, nil
}

func (f *fakeRunnerClient) cancelledExecutions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cancelled...)
}

func (f *fakeRunnerClient) init() {
	f.once.Do(func() {
		f.in = make(chan *runnerv1.ExecRequest, 8)
		f.out = make(chan *runnerv1.ExecResponse, 8)
	})
}

func (f *fakeRunnerClient) Exec(ctx context.Context, opts ...grpc.CallOption) (runnerv1.RunnerService_ExecClient, error) {
	f.init()
	f.mu.Lock()
	f.ctx = ctx
	f.mu.Unlock()
	return &fakeExecClient{ctx: ctx, in: f.in, out: f.out}, nil
}

func (f *fakeRunnerClient) recv(t *testing.T) *runnerv1.ExecRequest {
	t.Helper()
	f.init()
	select {
	case req := <-f.in:
		return req
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for exec request")
		return nil
	}
}

func (f *fakeRunnerClient) send(resp *runnerv1.ExecResponse) error {
	f.init()
	f.out <- resp
	return nil
}

func (f *fakeRunnerClient) contextDone() bool {
	f.mu.Lock()
	ctx := f.ctx
	f.mu.Unlock()
	if ctx == nil {
		return false
	}
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

type fakeExecClient struct {
	grpc.ClientStream
	ctx context.Context
	in  chan<- *runnerv1.ExecRequest
	out <-chan *runnerv1.ExecResponse
}

func (f *fakeExecClient) Send(req *runnerv1.ExecRequest) error {
	f.in <- req
	return nil
}

func (f *fakeExecClient) Recv() (*runnerv1.ExecResponse, error) {
	resp, ok := <-f.out
	if !ok {
		return nil, io.EOF
	}
	return resp, nil
}

func (f *fakeExecClient) CloseSend() error { return nil }

func (f *fakeExecClient) Context() context.Context { return f.ctx }

type fakePinger struct {
	mu    sync.Mutex
	pings int
	err   error
}

func (f *fakePinger) Ping(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pings++
	return f.err
}

func (f *fakePinger) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pings
}

func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func sandboxWorkload(workloadID, sandboxID string) *runnersv1.Workload {
	return &runnersv1.Workload{
		Meta:      &runnersv1.EntityMeta{Id: workloadID},
		OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:   sandboxID,
	}
}

func agentWorkload(workloadID, agentID string) *runnersv1.Workload {
	return &runnersv1.Workload{
		Meta:         &runnersv1.EntityMeta{Id: workloadID},
		OwnerKind:    runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:      uuid.NewString(),
		AgentClassId: stringPtr(agentID),
	}
}

func assertAuthorizationTuple(t *testing.T, req *authorizationv1.CheckRequest, user, relation, object string) {
	t.Helper()
	if req == nil || req.GetTupleKey() == nil {
		t.Fatal("expected authorization check")
	}
	tuple := req.GetTupleKey()
	if tuple.GetUser() != user || tuple.GetRelation() != relation || tuple.GetObject() != object {
		t.Fatalf("unexpected tuple user=%q relation=%q object=%q", tuple.GetUser(), tuple.GetRelation(), tuple.GetObject())
	}
}

func assertCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("expected code %s, got %s: %v", code, status.Code(err), err)
	}
}

func stringPtr(value string) *string { return &value }
