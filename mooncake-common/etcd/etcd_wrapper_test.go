package main

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func TestNewMaintenanceSessionCancelsStartup(t *testing.T) {
	original := startMaintenanceSession
	defer func() { startMaintenanceSession = original }()

	started := make(chan struct{})
	startMaintenanceSession = func(ctx context.Context, _ *clientv3.Client,
		_ int) (*concurrency.Session, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	result := make(chan error, 1)
	go func() {
		_, err := newMaintenanceSession(nil, 30, 10*time.Millisecond)
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("maintenance session startup was not attempted")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected startup cancellation error")
		}
	case <-time.After(time.Second):
		t.Fatal("maintenance session startup did not cancel")
	}
}

func TestStoreClientKeepaliveDoesNotPingIdleConnections(t *testing.T) {
	for _, name := range []string{mooncakeEtcdCACertEnv, mooncakeEtcdClientCertEnv,
		mooncakeEtcdClientKeyEnv, mooncakeEtcdServerNameEnv} {
		t.Setenv(name, "")
	}
	config, err := newStoreClientConfig([]string{"http://localhost:2379"})
	if err != nil {
		t.Fatal(err)
	}
	if config.PermitWithoutStream {
		t.Fatal("Store clients must not send keepalive PINGs without active streams")
	}
	if config.DialKeepAliveTime != 10*time.Second {
		t.Fatalf("active-stream keepalive interval = %v, want 10s", config.DialKeepAliveTime)
	}
	if config.DialKeepAliveTimeout != 3*time.Second {
		t.Fatalf("active-stream keepalive timeout = %v, want 3s", config.DialKeepAliveTimeout)
	}
}
