package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	agentsv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/runners/v1"
	terminalproxyv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/terminal_proxy/v1"
	"github.com/agynio/terminal-proxy/internal/runnerdial"
	"github.com/agynio/terminal-proxy/internal/ticket"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"nhooyr.io/websocket"
)

func TestIssueTicketReturnsSignedShortLivedTicket(t *testing.T) {
	store := ticket.NewStore([]byte("01234567890123456789012345678901"))
	runners := &fakeRunnersClient{}
	agents := &fakeAgentsClient{}
	server, err := New(Config{Tickets: store, TicketTTL: 30 * time.Second, WebSocketURL: "ws://proxy/terminal", Runners: runners, Agents: agents, RunnerDialer: &fakeRunnerDialer{}, TouchInterval: time.Second})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	identityID := uuid.NewString()
	workloadID := uuid.NewString()
	resp, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{IdentityId: identityID, WorkloadId: workloadID, ContainerName: "main"})
	if err != nil {
		t.Fatalf("issue ticket: %v", err)
	}
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
		Containers: []*runnersv1.Container{{Name: "main", ContainerId: "workload-" + workloadID, Status: runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING}},
	}}
	agents := &fakeAgentsClient{}
	dialer := &fakeRunnerDialer{runner: runner}
	server, err := New(Config{Tickets: store, TicketTTL: 30 * time.Second, WebSocketURL: "ws://proxy/terminal", Runners: runners, Agents: agents, RunnerDialer: dialer, TouchInterval: time.Hour})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	issued, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{
		IdentityId:    uuid.NewString(),
		WorkloadId:    workloadID,
		ContainerName: "main",
		Command:       &terminalproxyv1.TerminalCommand{Command: &terminalproxyv1.TerminalCommand_Shell{Shell: "echo ready"}},
	})
	if err != nil {
		t.Fatalf("issue ticket: %v", err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	wsURL := "ws" + httpServer.URL[len("http"):] + "/terminal?ticket=" + issued.GetTicket()
	conn, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.CloseNow()
	if err := conn.Write(context.Background(), websocket.MessageText, []byte(`{"cols":120,"rows":40}`)); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
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
}

func TestAgentWorkloadDoesNotTouchOrUpdateSandbox(t *testing.T) {
	store := ticket.NewStore([]byte("01234567890123456789012345678901"))
	workloadID := uuid.NewString()
	runner := &fakeRunnerClient{}
	runners := &fakeRunnersClient{workload: &runnersv1.Workload{
		Meta:       &runnersv1.EntityMeta{Id: workloadID},
		RunnerId:   uuid.NewString(),
		Status:     runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		OwnerKind:  runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE,
		OwnerId:    uuid.NewString(),
		Containers: []*runnersv1.Container{{Name: "main", ContainerId: "workload-" + workloadID}},
	}}
	agents := &fakeAgentsClient{}
	server, err := New(Config{Tickets: store, TicketTTL: 30 * time.Second, WebSocketURL: "ws://proxy/terminal", Runners: runners, Agents: agents, RunnerDialer: &fakeRunnerDialer{runner: runner}, TouchInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	issued, err := server.IssueTicket(context.Background(), &terminalproxyv1.IssueTicketRequest{IdentityId: uuid.NewString(), WorkloadId: workloadID, ContainerName: "main"})
	if err != nil {
		t.Fatalf("issue ticket: %v", err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	conn, _, err := websocket.Dial(context.Background(), "ws"+httpServer.URL[len("http"):]+"/terminal?ticket="+issued.GetTicket(), nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.CloseNow()
	if err := conn.Write(context.Background(), websocket.MessageText, []byte(`{"cols":80,"rows":24}`)); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
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

type fakeRunnersClient struct {
	workload *runnersv1.Workload
	mu       sync.Mutex
	touches  []string
}

func (f *fakeRunnersClient) GetWorkload(context.Context, *runnersv1.GetWorkloadRequest, ...grpc.CallOption) (*runnersv1.GetWorkloadResponse, error) {
	return &runnersv1.GetWorkloadResponse{Workload: f.workload}, nil
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
	once sync.Once
	in   chan *runnerv1.ExecRequest
	out  chan *runnerv1.ExecResponse
}

func (f *fakeRunnerClient) init() {
	f.once.Do(func() {
		f.in = make(chan *runnerv1.ExecRequest, 8)
		f.out = make(chan *runnerv1.ExecResponse, 8)
	})
}

func (f *fakeRunnerClient) Exec(context.Context, ...grpc.CallOption) (runnerv1.RunnerService_ExecClient, error) {
	f.init()
	return &fakeExecClient{ctx: context.Background(), in: f.in, out: f.out}, nil
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

var _ = timestamppb.Now
