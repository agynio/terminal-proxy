package zitimanager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/openziti/sdk-golang/ziti"
	"github.com/openziti/sdk-golang/ziti/edge"
)

var (
	newZitiContextFromFile = ziti.NewContextFromFile
	configureZitiContext   = configureZitiClientContext
)

type Manager struct {
	mu      sync.RWMutex
	zitiCtx ziti.Context
}

func NewFromFile(path string) (*Manager, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("ziti identity file missing")
	}
	zitiCtx, err := newZitiContextFromFile(path)
	if err != nil {
		return nil, fmt.Errorf("load ziti identity file: %w", err)
	}
	if err := configureZitiContext(zitiCtx); err != nil {
		zitiCtx.Close()
		return nil, fmt.Errorf("configure ziti context: %w", err)
	}
	return &Manager{zitiCtx: zitiCtx}, nil
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

func configureZitiClientContext(zitiCtx ziti.Context) error {
	return zitiCtx.Authenticate()
}
