//go:build cgo && linux && (amd64 || arm64)

package websocket_test

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	codexws "github.com/csbxd/gocodex/websocket"
)

func wsURL(server *httptest.Server) string { return "ws" + strings.TrimPrefix(server.URL, "http") }

func TestRejectedHandshakeMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, headers, body, want string
		status, limit             int
		truncated                 bool
	}{
		{"unauthorized", "Content-Length: 12\r\n", "auth failure", "auth failure", 401, 0, false},
		{"upgrade-required", "Content-Length: 4\r\n", "nope", "nope", 426, 0, false},
		{"rate-limit", "Content-Length: 9\r\n", "slow down", "slow down", 429, 0, false},
		{"bounded", "Content-Length: 12\r\n", "auth failure", "auth", 403, 4, true},
		{"incomplete", "Content-Length: 99\r\n", "partial", "partial", 403, 0, true},
		{"binary", "Content-Length: 3\r\n", "\x00\xffx", "\x00\xffx", 403, 0, false},
		{"chunked", "Transfer-Encoding: chunked\r\n", "4\r\noops\r\n0\r\n\r\n", "oops", 429, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				wire := fmt.Sprintf("HTTP/1.1 %d Rejected\r\n%sRetry-After: 7\r\nSet-Cookie: a=1\r\nSet-Cookie: b=2\r\n\r\n%s", tc.status, tc.headers, tc.body)
				_, _ = io.WriteString(conn, wire)
			}))
			defer server.Close()
			client := newClient(t, codexws.Options{NoProxy: true, MaxHandshakeBodyBytes: tc.limit})
			conn, hs, err := client.Dial(deadline(t), wsURL(server), nil)
			if conn != nil {
				_ = conn.Close()
				t.Fatal("rejected upgrade returned a connection")
			}
			var rejected *codexws.HandshakeError
			var native *codexws.Error
			if !errors.As(err, &rejected) || !errors.As(err, &native) || native.Kind != "handshake" {
				t.Fatalf("error types = %T %v", err, err)
			}
			if hs == nil || rejected.Handshake != hs || hs.StatusCode != tc.status || string(hs.Body) != tc.want || hs.BodyTruncated != tc.truncated {
				t.Fatalf("handshake = %#v", hs)
			}
			if hs.Headers.Get("Retry-After") != "7" || len(hs.Headers.Values("Set-Cookie")) != 2 {
				t.Fatal("error response headers were lost")
			}
			if strings.Contains(err.Error(), tc.body) && len(tc.body) > 4 {
				t.Fatal("error text disclosed response contents")
			}
		})
	}
}

func wsConnectProxy(t *testing.T, target string, tlsProxy bool, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "CONNECT" {
			t.Error("proxy did not receive CONNECT")
			w.WriteHeader(400)
			return
		}
		if r.Header.Get("Proxy-Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("user:p:@ss")) {
			t.Error("proxy authentication missing")
			w.WriteHeader(407)
			return
		}
		hits.Add(1)
		upstream, err := net.Dial("tcp", target)
		if err != nil {
			t.Error(err)
			w.WriteHeader(502)
			return
		}
		defer upstream.Close()
		conn, stream, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_, _ = io.WriteString(stream, "HTTP/1.1 200 Connection Established\r\n\r\n")
		if err := stream.Flush(); err != nil {
			return
		}
		done := make(chan struct{})
		go func() { _, _ = io.Copy(upstream, stream); _ = upstream.Close(); close(done) }()
		_, _ = io.Copy(conn, upstream)
		_ = conn.Close()
		<-done
	}))
	if tlsProxy {
		cert, root := testCertificate(t)
		server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
		file := filepath.Join(t.TempDir(), "proxy-root.pem")
		if err := os.WriteFile(file, root, 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("CODEX_CA_CERTIFICATE", file)
		server.StartTLS()
	} else {
		server.Start()
	}
	t.Cleanup(server.Close)
	return server
}

func TestExplicitProxyAndIsolation(t *testing.T) {
	origin := serve(t, echo)
	var aHits, bHits atomic.Int32
	a := wsConnectProxy(t, strings.TrimPrefix(origin.URL, "http://"), false, &aHits)
	b := wsConnectProxy(t, strings.TrimPrefix(origin.URL, "http://"), true, &bHits)
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("NO_PROXY", "*")
	var wg sync.WaitGroup
	for _, proxy := range []*httptest.Server{a, b} {
		parsed, _ := url.Parse(proxy.URL)
		parsed.User = url.UserPassword("user", "p:@ss")
		client := newClient(t, codexws.Options{ProxyURL: parsed.String()})
		ctx := deadline(t)
		wg.Go(func() {
			conn, _, err := client.Dial(ctx, "ws://unresolvable.invalid/echo", nil)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			if err := conn.WriteMessage(ctx, codexws.TextMessage, []byte("proxied")); err != nil {
				t.Error(err)
				return
			}
			_, data, err := conn.ReadMessage(ctx)
			if err != nil || string(data) != "proxied" {
				t.Errorf("echo=%q %v", data, err)
			}
		})
	}
	wg.Wait()
	if aHits.Load() != 1 || bHits.Load() != 1 {
		t.Fatalf("proxy hits=%d,%d", aHits.Load(), bHits.Load())
	}
	if os.Getenv("HTTP_PROXY") != "http://127.0.0.1:1" || os.Getenv("NO_PROXY") != "*" {
		t.Fatal("proxy configuration modified environment")
	}
	_ = dial(t, newClient(t, codexws.Options{NoProxy: true}), origin)
	a.Close()
	parsed, _ := url.Parse(a.URL)
	parsed.User = url.UserPassword("user", "p:@ss")
	client := newClient(t, codexws.Options{ProxyURL: parsed.String()})
	if conn, _, err := client.Dial(deadline(t), wsURL(origin), nil); err == nil {
		_ = conn.Close()
		t.Fatal("failed proxy fell back to direct")
	}
}

func TestExtendedOptionsValidation(t *testing.T) {
	for _, options := range []codexws.Options{
		{ProxyURL: "http://localhost:1", NoProxy: true}, {ProxyURL: "http://localhost:1", LoopbackDirect: true},
		{ProxyURL: "socks4://localhost:1"}, {ProxyURL: "http://user:secret@host/path"},
		{WriteFrameSize: -1}, {WriteFrameSize: 1<<20 + 1}, {MaxHandshakeBodyBytes: -1}, {MaxHandshakeBodyBytes: 1<<20 + 1},
	} {
		client, err := codexws.NewClient(options)
		if err == nil {
			_ = client.Close()
			t.Fatalf("invalid options accepted: %+v", options)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatal("password leaked in validation error")
		}
	}
	client := newClient(t, codexws.Options{NoProxy: true})
	conn := dial(t, client, serve(t, echo))
	if err := conn.WriteControl(deadline(t), codexws.TextMessage, nil); err == nil {
		t.Fatal("data accepted by WriteControl")
	}
	if err := conn.WriteControl(deadline(t), codexws.PingMessage, make([]byte, 126)); err == nil {
		t.Fatal("oversized control accepted")
	}
	if err := conn.WriteMessage(deadline(t), codexws.TextMessage, []byte("still usable")); err != nil {
		t.Fatal(err)
	}
	_, data, err := conn.ReadMessage(deadline(t))
	if err != nil || string(data) != "still usable" {
		t.Fatalf("invalid control poisoned connection: %q %v", data, err)
	}
}

func readClientFrame(reader io.Reader) (opcode byte, final bool, data []byte, err error) {
	var header [2]byte
	if _, err = io.ReadFull(reader, header[:]); err != nil {
		return
	}
	if header[1]&128 == 0 {
		err = errors.New("client frame is not masked")
		return
	}
	opcode, final = header[0]&15, header[0]&128 != 0
	size := uint64(header[1] & 127)
	if size == 126 {
		var length [2]byte
		_, err = io.ReadFull(reader, length[:])
		size = uint64(binary.BigEndian.Uint16(length[:]))
	} else if size == 127 {
		var length [8]byte
		_, err = io.ReadFull(reader, length[:])
		size = binary.BigEndian.Uint64(length[:])
	}
	if err != nil {
		return
	}
	if size > 32*1024 {
		err = fmt.Errorf("unexpected frame size %d", size)
		return
	}
	var mask [4]byte
	if _, err = io.ReadFull(reader, mask[:]); err != nil {
		return
	}
	data = make([]byte, int(size))
	if _, err = io.ReadFull(reader, data); err != nil {
		return
	}
	for i := range data {
		data[i] ^= mask[i%4]
	}
	return
}

func TestControlFramesInterruptFragmentedWrite(t *testing.T) {
	for _, mode := range []string{"automatic-pong", "explicit-ping", "close", "peer-close"} {
		t.Run(mode, func(t *testing.T) {
			first := make(chan struct{})
			result := make(chan error, 1)
			payload := bytes.Repeat([]byte("abcd"), 2<<20)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, stream, err := w.(http.Hijacker).Hijack()
				if err != nil {
					result <- err
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				digest := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
				_, _ = fmt.Fprintf(stream, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(digest[:]))
				if err = stream.Flush(); err != nil {
					result <- err
					return
				}
				seenControl, total := false, 0
				for {
					op, fin, data, err := readClientFrame(stream)
					if err != nil {
						result <- err
						return
					}
					if op == 1 || op == 0 {
						if total == 0 {
							close(first)
							if mode == "automatic-pong" {
								_, _ = conn.Write([]byte{0x89, 1, 'p'})
							}
							if mode == "peer-close" {
								body := codexws.ClosePayload(1009, "large")
								_, _ = conn.Write(append([]byte{0x88, byte(len(body))}, body...))
							}
						}
						if total+len(data) > len(payload) || !bytes.Equal(data, payload[total:total+len(data)]) {
							result <- errors.New("data fragments corrupted")
							return
						}
						total += len(data)
						if fin {
							if !seenControl || total != len(payload) {
								result <- fmt.Errorf("control was starved: control=%t bytes=%d", seenControl, total)
								return
							}
							result <- nil
							return
						}
					} else {
						want := byte(10)
						if mode == "explicit-ping" {
							want = 9
						}
						if mode == "close" || mode == "peer-close" {
							want = 8
						}
						if op != want || (op != 8 && string(data) != "p") {
							result <- fmt.Errorf("unexpected control %d %q", op, data)
							return
						}
						seenControl = true
						if op == 8 {
							if mode == "peer-close" {
								if !bytes.Equal(data, codexws.ClosePayload(1009, "large")) {
									result <- errors.New("Close acknowledgement lost status")
									return
								}
							} else {
								_, _ = conn.Write(append([]byte{0x88, byte(len(data))}, data...))
							}
							result <- nil
							return
						}
					}
				}
			}))
			defer server.Close()
			client := newClient(t, codexws.Options{NoProxy: true, WriteFrameSize: 1024})
			conn := dial(t, client, server)
			defer conn.Close()
			ctx, cancel := context.WithCancel(deadline(t))
			defer cancel()
			readDone := make(chan error, 1)
			if mode == "automatic-pong" || mode == "peer-close" {
				go func() {
					kind, data, err := conn.ReadMessage(ctx)
					if err == nil && mode == "automatic-pong" && (kind != codexws.PingMessage || string(data) != "p") {
						err = fmt.Errorf("incoming ping=%d %q", kind, data)
					}
					if err == nil && mode == "peer-close" && (kind != codexws.CloseMessage || !bytes.Equal(data, codexws.ClosePayload(1009, "large"))) {
						err = fmt.Errorf("peer Close lost: %d %q", kind, data)
					}
					readDone <- err
				}()
			}
			writeDone := make(chan error, 1)
			go func() { writeDone <- conn.WriteMessage(ctx, codexws.TextMessage, payload) }()
			await(t, first)
			if mode == "explicit-ping" {
				if err := conn.WriteControl(ctx, codexws.PingMessage, []byte("p")); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "close" {
				if err := conn.WriteControl(ctx, codexws.CloseMessage, codexws.ClosePayload(1000, "done")); err != nil {
					t.Fatal(err)
				}
				kind, data, err := conn.ReadMessage(ctx)
				if err != nil || kind != codexws.CloseMessage {
					t.Fatalf("peer close lost: %d %q %v", kind, data, err)
				}
			}
			if err := await(t, result); err != nil {
				t.Fatal(err)
			}
			errWrite := await(t, writeDone)
			if mode == "close" || mode == "peer-close" {
				if errWrite == nil {
					t.Fatal("data write continued after Close")
				}
			} else if errWrite != nil {
				t.Fatal(errWrite)
			}
			if mode == "automatic-pong" || mode == "peer-close" {
				if err := await(t, readDone); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestExplicitSOCKSProxy(t *testing.T) {
	for _, scheme := range []string{"socks5", "socks5h"} {
		t.Run(scheme, func(t *testing.T) {
			origin := serve(t, echo)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan error, 1)
			go func() { done <- serveWSOCKS(listener, strings.TrimPrefix(origin.URL, "http://")) }()
			t.Setenv("NO_PROXY", "*")
			client := newClient(t, codexws.Options{ProxyURL: scheme + "://usr:pw@" + listener.Addr().String()})
			conn, _, err := client.Dial(deadline(t), "ws://unresolvable.invalid/echo", nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := conn.WriteMessage(deadline(t), codexws.TextMessage, []byte("SOCKS")); err != nil {
				t.Fatal(err)
			}
			_, data, err := conn.ReadMessage(deadline(t))
			if err != nil || string(data) != "SOCKS" {
				t.Fatalf("SOCKS echo=%q %v", data, err)
			}
			_ = conn.Close()
			if err := await(t, done); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func serveWSOCKS(listener net.Listener, target string) error {
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	var greeting [2]byte
	if _, err = io.ReadFull(conn, greeting[:]); err != nil {
		return err
	}
	if greeting[0] != 5 {
		return errors.New("not SOCKS5")
	}
	if _, err = io.CopyN(io.Discard, conn, int64(greeting[1])); err != nil {
		return err
	}
	if _, err = conn.Write([]byte{5, 2}); err != nil {
		return err
	}
	var auth [2]byte
	if _, err = io.ReadFull(conn, auth[:]); err != nil {
		return err
	}
	user := make([]byte, int(auth[1]))
	if _, err = io.ReadFull(conn, user); err != nil {
		return err
	}
	var size [1]byte
	if _, err = io.ReadFull(conn, size[:]); err != nil {
		return err
	}
	password := make([]byte, int(size[0]))
	if _, err = io.ReadFull(conn, password); err != nil {
		return err
	}
	if auth[0] != 1 || string(user) != "usr" || string(password) != "pw" {
		return errors.New("SOCKS authentication missing")
	}
	if _, err = conn.Write([]byte{1, 0}); err != nil {
		return err
	}
	var request [4]byte
	if _, err = io.ReadFull(conn, request[:]); err != nil {
		return err
	}
	if request != [4]byte{5, 1, 0, 3} {
		return fmt.Errorf("SOCKS request = %v", request)
	}
	if _, err = io.ReadFull(conn, size[:]); err != nil {
		return err
	}
	host := make([]byte, int(size[0]))
	if _, err = io.ReadFull(conn, host); err != nil {
		return err
	}
	var port [2]byte
	if _, err = io.ReadFull(conn, port[:]); err != nil {
		return err
	}
	if string(host) != "unresolvable.invalid" || binary.BigEndian.Uint16(port[:]) != 80 {
		return errors.New("wrong SOCKS destination")
	}
	upstream, err := net.Dial("tcp", target)
	if err != nil {
		return err
	}
	defer upstream.Close()
	if _, err = conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0}); err != nil {
		return err
	}
	done := make(chan struct{})
	go func() { _, _ = io.Copy(upstream, conn); _ = upstream.Close(); close(done) }()
	_, _ = io.Copy(conn, upstream)
	_ = conn.Close()
	<-done
	return nil
}

func TestConcurrentFragmentedDataMessages(t *testing.T) {
	client := newClient(t, codexws.Options{NoProxy: true, WriteFrameSize: 1024})
	conn := dial(t, client, serve(t, echo))
	ctx := deadline(t)
	payloads := [][]byte{bytes.Repeat([]byte("多字节"), 50000), bytes.Repeat([]byte("other"), 50000)}
	done := make(chan error, 2)
	for _, payload := range payloads {
		go func() { done <- conn.WriteMessage(ctx, codexws.TextMessage, payload) }()
	}
	seen := map[string]bool{}
	for range payloads {
		kind, payload, err := conn.ReadMessage(ctx)
		if err != nil || kind != codexws.TextMessage {
			t.Fatalf("read=%d %v", kind, err)
		}
		if !bytes.Equal(payload, payloads[0]) && !bytes.Equal(payload, payloads[1]) {
			t.Fatal("data messages interleaved or UTF-8 fragments corrupted")
		}
		seen[string(payload)] = true
	}
	if len(seen) != 2 {
		t.Fatal("one data message was duplicated")
	}
	for range payloads {
		if err := await(t, done); err != nil {
			t.Fatal(err)
		}
	}
}
