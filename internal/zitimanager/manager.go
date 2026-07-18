package zitimanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	zitimgmtv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/ziti_management/v1"
	"github.com/openziti/sdk-golang/ziti"
	"github.com/openziti/sdk-golang/ziti/edge"
)

var (
	newZitiContext       = ziti.NewContext
	configureZitiContext = configureZitiClientContext
)

type Manager struct {
	mu         sync.RWMutex
	zitiCtx    ziti.Context
	client     zitimgmtv1.ZitiManagementServiceClient
	identityID string
}

func New(ctx context.Context, client zitimgmtv1.ZitiManagementServiceClient, enrollTimeout time.Duration) (*Manager, error) {
	if client == nil {
		return nil, errors.New("ziti management client missing")
	}
	if enrollTimeout <= 0 {
		return nil, errors.New("ziti enrollment timeout must be greater than 0")
	}
	manager := &Manager{client: client}
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

func (m *Manager) DialContext(ctx context.Context, service string) (edge.Conn, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.zitiCtx == nil {
		return nil, errors.New("ziti context missing")
	}
	return m.zitiCtx.DialContext(ctx, service)
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

func (m *Manager) RunLeaseRenewal(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			identityID := m.currentIdentityID()
			if identityID == "" {
				continue
			}
			_, _ = m.client.ExtendIdentityLease(ctx, &zitimgmtv1.ExtendIdentityLeaseRequest{ZitiIdentityId: identityID})
		}
	}
}

func (m *Manager) currentIdentityID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.identityID
}

func (m *Manager) enroll(ctx context.Context) (ziti.Context, string, error) {
	resp, err := m.client.RequestServiceIdentity(ctx, &zitimgmtv1.RequestServiceIdentityRequest{ServiceType: zitimgmtv1.ServiceType_SERVICE_TYPE_ORCHESTRATOR})
	if err != nil {
		return nil, "", fmt.Errorf("request ziti service identity: %w", err)
	}
	var cfg ziti.Config
	if err := json.Unmarshal(resp.GetIdentityJson(), &cfg); err != nil {
		return nil, "", fmt.Errorf("parse ziti identity json: %w", err)
	}
	zitiCtx, err := newZitiContext(&cfg)
	if err != nil {
		return nil, "", fmt.Errorf("create ziti context: %w", err)
	}
	if err := configureZitiContext(zitiCtx); err != nil {
		zitiCtx.Close()
		return nil, "", fmt.Errorf("configure ziti context: %w", err)
	}
	return zitiCtx, resp.GetZitiIdentityId(), nil
}

func configureZitiClientContext(zitiCtx ziti.Context) error {
	return zitiCtx.Authenticate()
}
