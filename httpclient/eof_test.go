//go:build cgo && linux && (amd64 || arm64)

package httpclient_test

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	codexhttp "github.com/csbxd/gocodex/httpclient"
)

func TestTransportPrematureEOF(t *testing.T) {
	const partial = "data: {\"type\":\"response.created\"}\n\n"
	const errorText = "connection closed before message completed; end of file before message length reached"
	for _, tc := range []struct {
		name, wire, body string
		headerError      bool
		unexpectedEOF    bool
		status           int
	}{
		{name: "before headers", headerError: true, unexpectedEOF: true},
		{name: "partial headers", wire: "HTTP/1.1 200 OK\r\nContent-Length:", headerError: true, unexpectedEOF: true},
		{name: "content length", wire: fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: %d\r\n\r\n%s", len(partial)+10, partial), body: partial, unexpectedEOF: true, status: 200},
		{name: "chunked", wire: fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n%x\r\n%s\r\n", len(partial), partial), body: partial, unexpectedEOF: true, status: 200},
		{name: "invalid headers", wire: "invalid HTTP response\r\n\r\n", headerError: true},
		{name: "complete body", wire: fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%s", len(partial), partial), body: partial, status: 200},
		{name: "close delimited", wire: "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n" + partial, body: partial, status: 200},
		{name: "HTTP error body", wire: fmt.Sprintf("HTTP/1.1 401 Unauthorized\r\nContent-Length: %d\r\n\r\n%s", len(errorText), errorText), body: errorText, status: 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				conn, writer, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = conn.Close() }()
				_, _ = writer.WriteString(tc.wire)
				_ = writer.Flush()
			}))
			t.Cleanup(server.Close)
			for _, direct := range []bool{false, true} {
				t.Run(fmt.Sprintf("direct=%t", direct), func(t *testing.T) {
					transport, err := codexhttp.NewTransport(codexhttp.Options{NoProxy: direct})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = transport.Close() })
					request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
					if err != nil {
						t.Fatal(err)
					}
					response, failure := transport.RoundTrip(request)
					if tc.headerError {
						if failure == nil || response != nil {
							t.Fatalf("header failure returned response=%v err=%v", response, failure)
						}
					} else {
						if failure != nil {
							t.Fatal(failure)
						}
						defer func() { _ = response.Body.Close() }()
						body, errRead := io.ReadAll(response.Body)
						failure = errRead
						if response.StatusCode != tc.status || string(body) != tc.body {
							t.Fatalf("response status=%d body=%q", response.StatusCode, body)
						}
						_, terminal := response.Body.Read(make([]byte, 1))
						if tc.unexpectedEOF {
							if !errors.Is(terminal, io.ErrUnexpectedEOF) {
								t.Errorf("terminal body error was not preserved: %v", terminal)
							}
						} else if failure != nil || terminal != io.EOF {
							t.Fatalf("complete response did not end cleanly: %v; terminal=%v", failure, terminal)
						}
					}
					if errors.Is(failure, io.ErrUnexpectedEOF) != tc.unexpectedEOF {
						t.Fatalf("unexpected EOF classification=%v, want %v: %v", errors.Is(failure, io.ErrUnexpectedEOF), tc.unexpectedEOF, failure)
					}
					if tc.unexpectedEOF {
						var native *codexhttp.Error
						if !errors.As(failure, &native) || native.Message == "" || errors.Is(failure, io.EOF) {
							t.Fatalf("premature EOF lost native details or became clean EOF: %v", failure)
						}
					}
				})
			}
		})
	}
}
