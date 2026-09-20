//go:build cgo && linux && (amd64 || arm64)

package websocket_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	codexws "github.com/csbxd/gocodex/websocket"
)

func TestHandshakeTimeoutImplementsNetError(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	client := newClient(t, codexws.Options{NoProxy: true, HandshakeTimeout: 50 * time.Millisecond})
	conn, response, err := client.Dial(deadline(t), "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if conn != nil || response != nil {
		t.Fatalf("timeout returned a connection or handshake: %v %v", conn, response)
	}
	var native *codexws.Error
	var network net.Error
	if !errors.As(err, &native) || native.Kind != "timeout" || !errors.As(err, &network) || !network.Timeout() || !network.Temporary() {
		t.Fatalf("handshake timeout is not a temporary net.Error: %T %v", err, err)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("native handshake timeout was confused with caller cancellation: %v", err)
	}
}
