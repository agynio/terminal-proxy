package runnerdial

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	runnerv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/runner/v1"
	"github.com/openziti/sdk-golang/ziti/edge"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	retryInitialBackoff = 500 * time.Millisecond
	retryMaxBackoff     = 10 * time.Second
	retryMaxAttempts    = 5
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
	NotifyAuthFailure(ctx context.Context)
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
			return dialZitiWithRetry(ctx, d.zitiContext, service)
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

// dialZitiWithRetry absorbs the transient failures of dialing an overlay
// service: a runner that has just come up may not have bound its terminator
// yet, and a stale api session surfaces as an auth error that resolves once the
// identity's lease is confirmed. Without this an attach fails outright on
// conditions that clear in under a second.
func dialZitiWithRetry(ctx context.Context, zitiCtx ZitiDialer, service string) (net.Conn, error) {
	backoff := retryInitialBackoff
	var lastErr error

	for attempt := 1; attempt <= retryMaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		conn, err := zitiCtx.DialContext(ctx, service)
		if err == nil {
			return conn, nil
		}
		if IsNoTerminators(err) {
			return nil, fmt.Errorf("dial ziti service %s: %w", service, err)
		}
		log.Printf("dial ziti service %s: attempt %d/%d failed: %v", service, attempt, retryMaxAttempts, err)
		lastErr = err
		if isAuthFailure(err) {
			zitiCtx.NotifyAuthFailure(ctx)
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		}
		if attempt == retryMaxAttempts {
			break
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		backoff *= 2
		if backoff > retryMaxBackoff {
			backoff = retryMaxBackoff
		}
	}
	return nil, fmt.Errorf("dial ziti service %s: %w", service, lastErr)
}

// IsNoTerminators reports a service with no bound terminator: the runner is
// unreachable rather than the dial being transiently unlucky, so callers stop
// rather than retry.
func IsNoTerminators(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no terminators")
}

func isAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	lowerMsg := strings.ToLower(msg)
	return strings.Contains(msg, "INVALID_AUTH") ||
		strings.Contains(msg, "no apiSession") ||
		strings.Contains(lowerMsg, "invalid session") ||
		strings.Contains(lowerMsg, "bad x25519 remote ecdh input")
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
