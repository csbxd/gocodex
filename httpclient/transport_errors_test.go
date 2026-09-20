//go:build cgo && linux && (amd64 || arm64)

package httpclient_test

import (
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"

	codexhttp "github.com/csbxd/gocodex/httpclient"
)

func TestTransportConnectionRefusedPreservesCause(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	for _, direct := range []bool{false, true} {
		name := "automatic"
		if direct {
			name = "direct"
		}
		t.Run(name, func(t *testing.T) {
			transport, err := codexhttp.NewTransport(codexhttp.Options{NoProxy: direct})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = transport.Close() })
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+address+"/private-path?token=private-token", nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = transport.RoundTrip(req)
			var native *codexhttp.Error
			if !errors.As(err, &native) || native.Kind != "request" || !strings.Contains(strings.ToLower(native.Message), "connection refused") {
				t.Fatalf("connection failure lost its cause: %T %v", err, err)
			}
			if strings.Contains(native.Message, address) || strings.Contains(native.Message, "private-") {
				t.Fatalf("native error retained the request URL: %v", native)
			}
		})
	}
}
