package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	agentsv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/runners/v1"
	terminalproxyv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/terminal_proxy/v1"
	zitimgmtv1 "github.com/agynio/terminal-proxy/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/terminal-proxy/internal/config"
	"github.com/agynio/terminal-proxy/internal/proxy"
	"github.com/agynio/terminal-proxy/internal/runnerdial"
	"github.com/agynio/terminal-proxy/internal/ticket"
	"github.com/agynio/terminal-proxy/internal/zitimanager"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("terminal-proxy: %v", err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}

	closeConn := func(conn *grpc.ClientConn) {
		if conn != nil {
			_ = conn.Close()
		}
	}
	runnersConn, err := grpc.NewClient(cfg.RunnersAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial runners: %w", err)
	}
	defer closeConn(runnersConn)
	agentsConn, err := grpc.NewClient(cfg.AgentsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial agents: %w", err)
	}
	defer closeConn(agentsConn)

	var (
		dialer      runnerdial.RunnerDialer
		zitiConn    *grpc.ClientConn
		zitiManager *zitimanager.Manager
	)
	if cfg.ZitiEnabled {
		zitiConn, err = grpc.NewClient(cfg.ZitiManagementAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial ziti management: %w", err)
		}
		defer closeConn(zitiConn)
		zitiClient := zitimgmtv1.NewZitiManagementServiceClient(zitiConn)
		zitiManager, err = zitimanager.New(ctx, zitiClient, cfg.ZitiEnrollmentTimeout)
		if err != nil {
			return err
		}
		defer zitiManager.Close()
		go zitiManager.RunLeaseRenewal(ctx, cfg.ZitiLeaseRenewalInterval)
		dialer = runnerdial.NewDialer(zitiManager)
	} else {
		runnerConn, err := grpc.NewClient(cfg.RunnerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial runner: %w", err)
		}
		defer closeConn(runnerConn)
		dialer = runnerdial.NewFallbackDialer(runnerv1.NewRunnerServiceClient(runnerConn))
	}
	defer dialer.Close()

	server, err := proxy.New(proxy.Config{
		Tickets:       ticket.NewStore(cfg.TicketSigningKey),
		TicketTTL:     cfg.TicketTTL,
		WebSocketURL:  cfg.PublicWebSocketURL,
		Runners:       runnersv1.NewRunnersServiceClient(runnersConn),
		Agents:        agentsv1.NewAgentsServiceClient(agentsConn),
		RunnerDialer:  dialer,
		TouchInterval: cfg.TouchInterval,
	})
	if err != nil {
		return err
	}

	grpcListener, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}
	httpServer := &http.Server{Addr: cfg.HTTPAddress, Handler: server}
	grpcServer := grpc.NewServer()
	terminalproxyv1.RegisterTerminalProxyServiceServer(grpcServer, server)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		<-groupCtx.Done()
		grpcServer.GracefulStop()
		return nil
	})
	group.Go(func() error {
		<-groupCtx.Done()
		return httpServer.Shutdown(context.Background())
	})
	group.Go(func() error {
		if err := grpcServer.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return err
		}
		return nil
	})
	group.Go(func() error {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	return group.Wait()
}
