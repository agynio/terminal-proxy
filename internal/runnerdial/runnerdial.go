package runnerdial

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	runnerv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/runner/v1"
	"github.com/openziti/sdk-golang/ziti/edge"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type RunnerClient interface {
	Exec(ctx context.Context, opts ...grpc.CallOption) (runnerv1.RunnerService_ExecClient, error)
	CancelExecution(ctx context.Context, in *runnerv1.CancelExecutionRequest, opts ...grpc.CallOption) (*runnerv1.CancelExecutionResponse, error)
}

type RunnerDialer interface {
	Dial(ctx context.Context, runnerID string) (RunnerClient, error)
	Close()
}

type ZitiDialer interface {
	DialContext(ctx context.Context, service string) (edge.Conn, error)
}

type Dialer struct {
	zitiContext ZitiDialer
	mu          sync.Mutex
	conns       map[string]*grpc.ClientConn
}

func NewDialer(zitiContext ZitiDialer) *Dialer {
	return &Dialer{zitiContext: zitiContext, conns: map[string]*grpc.ClientConn{}}
}

func (d *Dialer) Dial(ctx context.Context, runnerID string) (RunnerClient, error) {
	runnerID = strings.TrimSpace(runnerID)
	if runnerID == "" {
		return nil, errors.New("runner id missing")
	}
	if d.zitiContext == nil {
		return nil, errors.New("ziti context missing")
	}
	if conn := d.connection(runnerID); conn != nil {
		return runnerv1.NewRunnerServiceClient(conn), nil
	}
	conn, err := d.dialRunner(ctx, runnerID)
	if err != nil {
		return nil, err
	}
	conn = d.storeConnection(runnerID, conn)
	return runnerv1.NewRunnerServiceClient(conn), nil
}

func (d *Dialer) Close() {
	d.mu.Lock()
	conns := d.conns
	d.conns = map[string]*grpc.ClientConn{}
	d.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (d *Dialer) connection(runnerID string) *grpc.ClientConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	conn := d.conns[runnerID]
	if conn == nil {
		return nil
	}
	if badState(conn.GetState()) {
		delete(d.conns, runnerID)
		_ = conn.Close()
		return nil
	}
	return conn
}

func (d *Dialer) storeConnection(runnerID string, conn *grpc.ClientConn) *grpc.ClientConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	existing := d.conns[runnerID]
	if existing != nil && !badState(existing.GetState()) {
		_ = conn.Close()
		return existing
	}
	if existing != nil {
		_ = existing.Close()
	}
	d.conns[runnerID] = conn
	return conn
}

func (d *Dialer) dialRunner(ctx context.Context, runnerID string) (*grpc.ClientConn, error) {
	service := "runner-" + runnerID
	conn, err := grpc.NewClient(
		"passthrough:///"+service,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return d.zitiContext.DialContext(ctx, service)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial runner %s: %w", runnerID, err)
	}
	return conn, nil
}

func badState(state connectivity.State) bool {
	return state == connectivity.Shutdown || state == connectivity.TransientFailure
}

type FallbackDialer struct {
	runner runnerv1.RunnerServiceClient
}

func NewFallbackDialer(runner runnerv1.RunnerServiceClient) *FallbackDialer {
	return &FallbackDialer{runner: runner}
}

func (f *FallbackDialer) Dial(context.Context, string) (RunnerClient, error) {
	if f.runner == nil {
		return nil, errors.New("runner client missing")
	}
	return f.runner, nil
}

func (f *FallbackDialer) Close() {}
