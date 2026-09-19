//go:build cgo && linux && (amd64 || arm64)

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/csbxd/gocodex/httpclient"
	"github.com/csbxd/gocodex/websocket"
	gorilla "github.com/gorilla/websocket"
)

// Exercise both transports inside one process and verify that closing a client
// leaves the shared runtime and the other transport's handles usable.
func TestSharedBridgeAndIndependentClientLifetimes(t *testing.T) {
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy", "CODEX_CA_CERTIFICATE", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		t.Setenv(key, "")
	}
	pending := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ws":
			upgrader := gorilla.Upgrader{}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			for {
				kind, data, err := conn.ReadMessage()
				if err != nil {
					return
				}
				if conn.WriteMessage(kind, data) != nil {
					return
				}
			}
		case "/pending":
			close(pending)
			<-r.Context().Done()
		default:
			_, _ = io.WriteString(w, r.URL.Path)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	hc, err := httpclient.NewClient(httpclient.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer hc.Close()
	wc, err := websocket.NewClient(websocket.Options{LoopbackDirect: true})
	if err != nil {
		t.Fatal(err)
	}
	defer wc.Close()
	conn, _, err := wc.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	get := func(client *httpclient.Client, path string) error {
		response, err := client.Get(ctx, server.URL+path)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err == nil && string(data) != path {
			return fmt.Errorf("HTTP body: got %q, want %q", data, path)
		}
		return err
	}
	echo := func(text string) error {
		if err := conn.WriteMessage(ctx, websocket.TextMessage, []byte(text)); err != nil {
			return err
		}
		kind, data, err := conn.ReadMessage(ctx)
		if err == nil && (kind != websocket.TextMessage || string(data) != text) {
			return fmt.Errorf("WebSocket message: type=%d, data=%q", kind, data)
		}
		return err
	}
	var wg sync.WaitGroup
	for worker := range 4 {
		wg.Go(func() {
			for i := range 16 {
				if err := get(hc, fmt.Sprintf("/request/%d/%d", worker, i)); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Go(func() {
		for i := range 32 {
			if err := echo(fmt.Sprintf("message-%d", i)); err != nil {
				t.Error(err)
				return
			}
		}
	})
	wg.Wait()

	done := make(chan error, 1)
	go func() { done <- get(hc, "/pending") }()
	select {
	case <-pending:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := hc.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, httpclient.ErrClientClosed) {
			t.Fatalf("HTTP cancellation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := echo("HTTP client is closed; WebSocket remains usable"); err != nil {
		t.Fatal(err)
	}
	if err := wc.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := httpclient.NewClient(httpclient.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := get(replacement, "/runtime-still-usable"); err != nil {
		t.Fatal(err)
	}
}
