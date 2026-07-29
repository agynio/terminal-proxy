package zitimanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	zitimgmtv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/ziti_management/v1"
	"github.com/openziti/sdk-golang/ziti"
	"github.com/openziti/sdk-golang/ziti/edge"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	retryInitialBackoff = 1 * time.Second
	retryMaxBackoff     = 15 * time.Second
	leaseRetryBackoff   = []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}

	newZitiContext       = ziti.NewContext
	configureZitiContext = configureZitiClientContext
)

// ErrIdentityLost reports that the Terminal Proxy's OpenZiti identity no longer
// exists (garbage-collected while the pod was running). Recovery is a pod
// restart: a fresh pod enrolls a fresh identity.
var ErrIdentityLost = errors.New("ziti identity lost")

// Manager holds the Terminal Proxy's OpenZiti identity and keeps its lease
// alive. The identity is ephemeral and requested at startup rather than mounted
// from a Secret: it carries the terminal-proxy-hosts role attribute, which the
// static terminal-proxy-dial-runners policy grants against #runner-services, so
// exec streams can reach a runner in any network rather than only a runner
// sharing this cluster.
type Manager struct {
	mu              sync.RWMutex
	zitiCtx         ziti.Context
	identityID      string
	mgmtClient      zitimgmtv1.ZitiManagementServiceClient
	renewalInterval time.Duration
	enrollTimeout   time.Duration

	lostOnce sync.Once
	lostCh   chan error
}

func New(ctx context.Context, client zitimgmtv1.ZitiManagementServiceClient, enrollTimeout, renewalInterval time.Duration) (*Manager, error) {
	if client == nil {
		return nil, errors.New("ziti management client missing")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if enrollTimeout <= 0 {
		return nil, fmt.Errorf("enroll timeout must be greater than 0")
	}
	if renewalInterval <= 0 {
		return nil, fmt.Errorf("lease renewal interval must be greater than 0")
	}
	manager := &Manager{
		mgmtClient:      client,
		renewalInterval: renewalInterval,
		enrollTimeout:   enrollTimeout,
		lostCh:          make(chan error, 1),
	}
	enrollCtx, cancel := context.WithTimeout(ctx, enrollTimeout)
	defer cancel()
	zitiCtx, identityID, err := manager.enroll(enrollCtx)
	if err != nil {
		return nil, err
	}
	manager.zitiCtx = zitiCtx
	manager.identityID = identityID
	return manager, nil
}

// IdentityLost is signalled once, with an error wrapping ErrIdentityLost, when
// the identity is found to be gone (definitive NOT_FOUND from the lease
// extension, or an auth failure confirmed to be identity loss). The caller must
// log the error and terminate so the pod restart path enrolls a fresh identity.
func (m *Manager) IdentityLost() <-chan error {
	return m.lostCh
}

func (m *Manager) signalIdentityLost(err error) {
	m.lostOnce.Do(func() {
		m.lostCh <- err
	})
}

func (m *Manager) DialContext(ctx context.Context, service string) (edge.Conn, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.zitiCtx == nil {
		return nil, errors.New("ziti context missing")
	}
	return m.zitiCtx.DialContext(ctx, service)
}

// NotifyAuthFailure is called by the dial path when a dial is rejected for
// authentication reasons. It confirms whether the identity is truly gone by
// attempting a lease extension; a definitive NOT_FOUND signals identity loss.
// A transient auth failure (identity still present) is logged and ignored.
func (m *Manager) NotifyAuthFailure(ctx context.Context) {
	err := m.extendLeaseWithRetry(ctx)
	if err == nil {
		return
	}
	if isNotFoundGrpcError(err) {
		m.signalIdentityLost(fmt.Errorf("%w: identity %s no longer exists (auth failure): %v", ErrIdentityLost, m.currentIdentityID(), err))
		return
	}
	if ctx.Err() == nil {
		log.Printf("ziti auth failure check for identity %s: %v", m.currentIdentityID(), err)
	}
}

func (m *Manager) RunLeaseRenewal(ctx context.Context) {
	ticker := time.NewTicker(m.renewalInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			err := m.extendLeaseWithRetry(ctx)
			if err == nil {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			if isNotFoundGrpcError(err) {
				m.signalIdentityLost(fmt.Errorf("%w: identity %s no longer exists: %v", ErrIdentityLost, m.currentIdentityID(), err))
				return
			}
			log.Printf("failed to extend ziti lease for identity %s: %v", m.currentIdentityID(), err)
		}
	}
}

func (m *Manager) extendLeaseWithRetry(ctx context.Context) error {
	identityID := m.currentIdentityID()
	if identityID == "" {
		return errors.New("ziti identity id missing")
	}
	var lastErr error
	for attempt := 0; attempt <= len(leaseRetryBackoff); attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, err := m.mgmtClient.ExtendIdentityLease(ctx, &zitimgmtv1.ExtendIdentityLeaseRequest{ZitiIdentityId: identityID})
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryableGrpcError(err) {
			return err
		}
		if attempt == len(leaseRetryBackoff) {
			break
		}
		if waitErr := waitWithContext(ctx, leaseRetryBackoff[attempt]); waitErr != nil {
			return waitErr
		}
	}
	return lastErr
}

func (m *Manager) currentIdentityID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.identityID
}

func (m *Manager) Close() {
	m.mu.Lock()
	zitiCtx := m.zitiCtx
	m.zitiCtx = nil
	m.mu.Unlock()
	if zitiCtx != nil {
		zitiCtx.Close()
	}
}

func (m *Manager) enroll(ctx context.Context) (ziti.Context, string, error) {
	var identityResp *zitimgmtv1.RequestServiceIdentityResponse
	if err := retryWithBackoff(ctx, "ziti enrollment", func(attemptCtx context.Context) error {
		var requestErr error
		identityResp, requestErr = m.mgmtClient.RequestServiceIdentity(attemptCtx, &zitimgmtv1.RequestServiceIdentityRequest{
			ServiceType: zitimgmtv1.ServiceType_SERVICE_TYPE_TERMINAL_PROXY,
		})
		return requestErr
	}); err != nil {
		return nil, "", fmt.Errorf("request ziti service identity: %w", err)
	}
	identityID := identityResp.GetZitiIdentityId()
	if identityID == "" {
		return nil, "", fmt.Errorf("request ziti service identity: missing identity id")
	}
	identityJSON := identityResp.GetIdentityJson()
	if len(identityJSON) == 0 {
		return nil, "", fmt.Errorf("request ziti service identity: missing identity json")
	}
	identityConfig := &ziti.Config{}
	if err := json.Unmarshal(identityJSON, identityConfig); err != nil {
		return nil, "", fmt.Errorf("parse ziti identity: %w", err)
	}
	zitiCtx, err := newZitiContext(identityConfig)
	if err != nil {
		return nil, "", fmt.Errorf("load ziti identity: %w", err)
	}
	if err := configureZitiContext(zitiCtx); err != nil {
		return nil, "", err
	}
	return zitiCtx, identityID, nil
}

func configureZitiClientContext(zitiCtx ziti.Context) error {
	ctxImpl, ok := zitiCtx.(*ziti.ContextImpl)
	if !ok {
		return fmt.Errorf("unexpected ziti context type %T; cannot configure client context", zitiCtx)
	}
	ctxImpl.CtrlClt.SetUseOidc(false)
	ctxImpl.CtrlClt.ConfigTypes = append(ctxImpl.CtrlClt.ConfigTypes, ziti.InterceptV1, ziti.ClientConfigV1)
	return nil
}

func retryWithBackoff(ctx context.Context, operationName string, fn func(context.Context) error) error {
	backoff := retryInitialBackoff
	attempt := 1
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if !isRetryableGrpcError(err) {
			return err
		}

		delay := backoff
		if delay > retryMaxBackoff {
			delay = retryMaxBackoff
		}

		log.Printf("%s failed (attempt %d), retrying in %s: %v", operationName, attempt, delay, err)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		backoff *= 2
		if backoff > retryMaxBackoff {
			backoff = retryMaxBackoff
		}
		attempt++
	}
}

func isRetryableGrpcError(err error) bool {
	statusErr, ok := status.FromError(err)
	if !ok {
		return false
	}
	return statusErr.Code() == codes.Unavailable || statusErr.Code() == codes.Unknown
}

func isNotFoundGrpcError(err error) bool {
	statusErr, ok := status.FromError(err)
	return ok && statusErr.Code() == codes.NotFound
}

func waitWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
