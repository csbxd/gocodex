//go:build cgo && linux && (amd64 || arm64)

package websocket_test

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	codexws "github.com/csbxd/gocodex/websocket"
	"github.com/gorilla/websocket"
)

func TestHandshakeEOFPreservesClassification(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	t.Cleanup(server.Close)
	client := newClient(t, codexws.Options{NoProxy: true})
	conn, response, err := client.Dial(deadline(t), "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	var native *codexws.Error
	if conn != nil || response != nil || !errors.Is(err, io.ErrUnexpectedEOF) || !errors.As(err, &native) {
		t.Fatalf("handshake EOF lost native/standard classification: %T %v", err, err)
	}
}

func TestPeerCloseSurvivesFailedAcknowledgement(t *testing.T) {
	for _, sendClose := range []bool{true, false} {
		name := "TCP drop without Close"
		if sendClose {
			name = "Close 1009 then TCP drop"
		}
		t.Run(name, func(t *testing.T) { testPeerCloseAcknowledgement(t, sendClose) })
	}
}

func testPeerCloseAcknowledgement(t *testing.T, sendClose bool) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		_, reader, err := conn.NextReader()
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := io.CopyN(io.Discard, reader, 1024); err != nil {
			t.Error(err)
			return
		}
		if sendClose {
			if err := conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(1009, "too big"), time.Now().Add(time.Second)); err != nil {
				t.Error(err)
			}
		}
		// Closing without draining the upload can make the client's automatic
		// acknowledgement fail with EPIPE or ECONNRESET.
	}))
	t.Cleanup(server.Close)
	conn := dial(t, newClient(t, codexws.Options{NoProxy: true}), server)
	t.Cleanup(func() { _ = conn.Close() })
	ctx := deadline(t)
	type result struct {
		kind codexws.MessageType
		data []byte
		err  error
	}
	read := make(chan result, 1)
	go func() {
		kind, data, err := conn.ReadMessage(ctx)
		read <- result{kind, data, err}
	}()
	_ = conn.WriteMessage(ctx, codexws.TextMessage, bytes.Repeat([]byte("x"), 8<<20))
	got := <-read
	if !sendClose {
		if got.err == nil || got.kind == codexws.CloseMessage {
			t.Fatalf("transport failure invented a peer Close: kind=%d err=%v", got.kind, got.err)
		}
		return
	}
	if got.err != nil || got.kind != codexws.CloseMessage || !bytes.Equal(got.data, codexws.ClosePayload(1009, "too big")) {
		t.Fatalf("peer Close was replaced by acknowledgement/write failure: kind=%d data=%q err=%v", got.kind, got.data, got.err)
	}
	if _, _, err := conn.ReadMessage(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("read after peer Close = %v, want EOF", err)
	}
}
