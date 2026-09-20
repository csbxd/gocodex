//go:build cgo && linux && (amd64 || arm64)

package httpclient_test

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	codexhttp "github.com/csbxd/gocodex/httpclient"
)

func TestHandshakeFailurePreservesTransportType(t *testing.T) {
	for _, tlsAlert := range []bool{false, true} {
		for _, direct := range []bool{false, true} {
			if !tlsAlert && direct {
				continue
			}
			name := "SOCKS"
			if tlsAlert {
				name = "TLS automatic"
				if direct {
					name = "TLS direct"
				}
			}
			t.Run(name, func(t *testing.T) {
				endpoint, proxyURL := failingHandshakeServer(t, tlsAlert)
				transport, err := codexhttp.NewTransport(codexhttp.Options{NoProxy: direct, ProxyURL: proxyURL})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = transport.Close() })
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, endpoint, nil)
				if err != nil {
					t.Fatal(err)
				}
				_, failure := transport.RoundTrip(request)
				var op *net.OpError
				var native *codexhttp.Error
				if !errors.As(failure, &op) || !errors.As(failure, &native) || native.Kind != "connect" {
					t.Fatalf("handshake failure lost transport type: %T %v", failure, failure)
				}
				if tlsAlert && !strings.Contains(strings.ToLower(native.Message), "internal error") {
					t.Fatalf("TLS alert details lost: %v", failure)
				}
				if !tlsAlert && !strings.Contains(native.Message, "server failure") {
					t.Fatalf("SOCKS failure details lost: %v", failure)
				}
			})
		}
	}
}

func failingHandshakeServer(t *testing.T, tlsAlert bool) (string, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	t.Cleanup(func() { _ = listener.Close(); <-done })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if tlsAlert {
			var header [5]byte
			_, err = io.ReadFull(conn, header[:])
			if err == nil {
				_, err = io.CopyN(io.Discard, conn, int64(binary.BigEndian.Uint16(header[3:])))
			}
			if err == nil {
				_, err = conn.Write([]byte{21, 3, 3, 0, 2, 2, 80})
			}
		} else {
			var greeting [2]byte
			_, err = io.ReadFull(conn, greeting[:])
			if err == nil {
				_, err = io.CopyN(io.Discard, conn, int64(greeting[1]))
			}
			if err == nil {
				_, err = conn.Write([]byte{5, 0})
			}
			var command [4]byte
			if err == nil {
				_, err = io.ReadFull(conn, command[:])
			}
			length := 4
			if command[3] == 4 {
				length = 16
			} else if command[3] == 3 {
				var size [1]byte
				_, err = io.ReadFull(conn, size[:])
				length = int(size[0])
			}
			if err == nil {
				_, err = io.CopyN(io.Discard, conn, int64(length+2))
			}
			if err == nil {
				_, err = conn.Write([]byte{5, 1, 0, 1, 0, 0, 0, 0, 0, 0})
			}
		}
		done <- err
	}()
	if tlsAlert {
		return "https://" + listener.Addr().String(), ""
	}
	return "http://127.0.0.1:80/responses", "socks5://" + listener.Addr().String()
}
