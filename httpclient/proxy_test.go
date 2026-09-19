//go:build cgo && linux && (amd64 || arm64)

package httpclient_test

import (
	"context"
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

	"github.com/csbxd/gocodex/httpclient"
)

func TestExplicitProxyIsolationAndPrecedence(t *testing.T) {
	var aHits, bHits atomic.Int32
	proxy := func(name string, hits *atomic.Int32) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !r.URL.IsAbs() || r.URL.Host != "unresolvable.invalid" {
				t.Errorf("proxy URI = %s", r.URL)
			}
			hits.Add(1)
			_, _ = io.WriteString(w, name)
		}))
	}
	a, b := proxy("a", &aHits), proxy("b", &bHits)
	defer a.Close()
	defer b.Close()
	// An explicit proxy must override both an unusable environment proxy and
	// NO_PROXY=*, without changing either process-wide setting.
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("NO_PROXY", "*")
	transports := []*httpclient.Transport{
		testTransport(t, httpclient.Options{ProxyURL: a.URL}),
		testTransport(t, httpclient.Options{ProxyURL: b.URL, ProxyPolicy: httpclient.ReqwestDefault}),
	}
	var wg sync.WaitGroup
	for i, transport := range transports {
		wg.Go(func() {
			client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
			for range 16 {
				response, err := client.Get("http://unresolvable.invalid/path")
				if err != nil {
					t.Error(err)
					return
				}
				body, err := io.ReadAll(response.Body)
				_ = response.Body.Close()
				if err != nil || string(body) != []string{"a", "b"}[i] {
					t.Errorf("proxy result = %q %v", body, err)
				}
			}
		})
	}
	wg.Wait()
	if aHits.Load() != 16 || bHits.Load() != 16 {
		t.Fatalf("proxy requests: a=%d b=%d", aHits.Load(), bHits.Load())
	}
	if os.Getenv("HTTP_PROXY") != "http://127.0.0.1:1" || os.Getenv("NO_PROXY") != "*" {
		t.Fatal("client modified process proxy environment")
	}
}

func TestExplicitProxyAuthenticationAndRedirect(t *testing.T) {
	var hits atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:p:@ss"))
		if r.Header.Get("Proxy-Authorization") != want {
			t.Error("missing/incorrect proxy authentication")
		}
		count := hits.Add(1)
		if count == 1 && (r.Header.Get("Authorization") != "Bearer origin-secret" || r.Header.Get("Cookie") != "session=private") {
			t.Error("initial origin credentials missing")
		}
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "http://second.invalid/end", http.StatusFound)
			return
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Referer") != "http://first.invalid/" {
			t.Error("cross-origin redirect leaked credentials or query data")
		}
		_, _ = io.WriteString(w, r.URL.Host+r.URL.Path)
	}))
	defer proxy.Close()
	proxyURL, _ := url.Parse(proxy.URL)
	proxyURL.User = url.UserPassword("user", "p:@ss")
	client := testClient(t, httpclient.Options{ProxyURL: proxyURL.String()})
	response, err := client.Do(context.Background(), httpclient.Request{
		URL:     "http://first.invalid/start?token=origin-secret",
		Headers: http.Header{"Authorization": {"Bearer origin-secret"}, "Cookie": {"session=private"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if body := string(readBody(t, response)); body != "second.invalid/end" || hits.Load() != 2 {
		t.Fatalf("redirect = %q, hits=%d", body, hits.Load())
	}
	transport := testTransport(t, httpclient.Options{ProxyURL: proxyURL.String()})
	standard := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response2, err := standard.Get("http://first.invalid/start")
	if err != nil {
		t.Fatal(err)
	}
	_ = response2.Body.Close()
	if response2.StatusCode != 302 || hits.Load() != 3 {
		t.Fatal("native proxy client bypassed CheckRedirect")
	}
}

func TestNoProxyAndNoDirectFallback(t *testing.T) {
	var originHits, proxyHits atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originHits.Add(1)
		_, _ = io.WriteString(w, "direct")
	}))
	defer origin.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyHits.Add(1)
		_, _ = io.WriteString(w, "proxy")
	}))
	t.Setenv("HTTP_PROXY", proxy.URL)
	defer proxy.Close()
	client := testClient(t, httpclient.Options{NoProxy: true})
	response, err := client.Get(context.Background(), origin.URL)
	if err != nil {
		t.Fatal(err)
	}
	if body := string(readBody(t, response)); body != "direct" || proxyHits.Load() != 0 {
		t.Fatalf("NoProxy = %q hits=%d", body, proxyHits.Load())
	}
	badProxy := proxy.URL
	proxy.Close()
	explicit := testClient(t, httpclient.Options{ProxyURL: badProxy, Timeout: time.Second})
	if unexpected, err := explicit.Get(context.Background(), origin.URL); err == nil {
		_ = unexpected.Body.Close()
		t.Fatal("failed proxy silently fell back to a direct connection")
	}
	if originHits.Load() != 1 {
		t.Fatal("request bypassed the configured proxy")
	}
}

func TestExplicitProxyRedirectBody(t *testing.T) {
	for _, status := range []int{302, 307} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				if r.URL.Path == "/start" {
					http.Redirect(w, r, "/end", status)
					return
				}
				wantMethod, wantBody, wantType := "POST", `{"value":123}`, "application/json"
				if status == 302 {
					wantMethod, wantBody, wantType = "GET", "", ""
				}
				if r.Method != wantMethod || string(body) != wantBody || r.Header.Get("Content-Type") != wantType {
					t.Errorf("redirect request = %s %q %q", r.Method, body, r.Header.Get("Content-Type"))
				}
				_, _ = io.WriteString(w, "done")
			}))
			defer proxy.Close()
			client := testClient(t, httpclient.Options{ProxyURL: proxy.URL})
			response, err := client.PostJSON(context.Background(), "http://unresolvable.invalid/start", map[string]int{"value": 123})
			if err != nil {
				t.Fatal(err)
			}
			if string(readBody(t, response)) != "done" || response.URL != "http://unresolvable.invalid/end" {
				t.Fatal("incorrect redirect response")
			}
		})
	}
}

func TestExplicitProxyTimeoutAndCancellation(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, sdkTimeout := range []bool{false, true} {
			t.Run(fmt.Sprintf("streaming=%t/sdkTimeout=%t", streaming, sdkTimeout), func(t *testing.T) {
				started, left := make(chan struct{}), make(chan struct{})
				proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/start" {
						http.Redirect(w, r, "/wait", http.StatusFound)
						return
					}
					if streaming {
						w.WriteHeader(200)
						w.(http.Flusher).Flush()
					}
					close(started)
					<-r.Context().Done()
					close(left)
				}))
				defer proxy.Close()
				options := httpclient.Options{ProxyURL: proxy.URL}
				if sdkTimeout {
					options.Timeout = 300 * time.Millisecond
				}
				client := testClient(t, options)
				defer client.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					response, err := client.Get(ctx, "http://unresolvable.invalid/start")
					if err == nil {
						_, err = io.ReadAll(response.Body)
						_ = response.Body.Close()
					}
					done <- err
				}()
				await(t, started)
				if !sdkTimeout {
					cancel()
				}
				err := await(t, done)
				if sdkTimeout {
					var native *httpclient.Error
					if !errors.As(err, &native) || !native.Timeout() {
						t.Fatalf("expected timeout, got %v", err)
					}
				} else if !errors.Is(err, context.Canceled) {
					t.Fatalf("expected cancellation, got %v", err)
				}
				await(t, left)
			})
		}
	}
}

// Serve a real CONNECT tunnel. The dial target is a local TLS fixture, keeping
// tests offline while still checking the requested proxy authority and auth.
func connectProxyHandler(t *testing.T, target string, hits *atomic.Int32) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || r.Host != target {
			t.Errorf("CONNECT = %s %s", r.Method, r.Host)
			http.Error(w, "bad CONNECT", 400)
			return
		}
		if r.Header.Get("Proxy-Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("user:pass")) {
			t.Error("CONNECT auth missing")
			http.Error(w, "auth", 407)
			return
		}
		upstream, err := net.DialTimeout("tcp", target, time.Second)
		if err != nil {
			t.Error(err)
			http.Error(w, "dial", 502)
			return
		}
		defer upstream.Close()
		conn, stream, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		hits.Add(1)
		_, _ = io.WriteString(stream, "HTTP/1.1 200 Connection Established\r\n\r\n")
		if stream.Flush() != nil {
			return
		}
		finished := make(chan struct{})
		go func() { _, _ = io.Copy(upstream, stream); _ = upstream.Close(); close(finished) }()
		_, _ = io.Copy(conn, upstream)
		_ = conn.Close()
		<-finished
	})
}

func TestExplicitHTTPAndHTTPSProxyCONNECT(t *testing.T) {
	for _, secureProxy := range []bool{false, true} {
		t.Run(fmt.Sprintf("proxyTLS=%t", secureProxy), func(t *testing.T) {
			origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Proxy-Authorization") != "" {
					t.Error("proxy credentials leaked to origin")
				}
				_, _ = io.WriteString(w, "verified tunnel")
			}))
			cert, root := testCertificate(t)
			origin.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
			origin.StartTLS()
			defer origin.Close()
			var hits atomic.Int32
			proxy := httptest.NewUnstartedServer(connectProxyHandler(t, strings.TrimPrefix(origin.URL, "https://"), &hits))
			if secureProxy {
				proxyCert, proxyRoot := testCertificate(t)
				root = append(root, proxyRoot...)
				proxy.TLS = &tls.Config{Certificates: []tls.Certificate{proxyCert}}
				proxy.StartTLS()
			} else {
				proxy.Start()
			}
			defer proxy.Close()
			file := filepath.Join(t.TempDir(), "roots.pem")
			if err := os.WriteFile(file, root, 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("CODEX_CA_CERTIFICATE", file)
			t.Setenv("NO_PROXY", "*")
			proxyURL, _ := url.Parse(proxy.URL)
			proxyURL.User = url.UserPassword("user", "pass")
			transport := testTransport(t, httpclient.Options{ProxyURL: proxyURL.String()})
			defer transport.Close()
			response, err := (&http.Client{Transport: transport, Timeout: 5 * time.Second}).Get(origin.URL)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if err != nil || string(body) != "verified tunnel" || hits.Load() != 1 {
				t.Fatalf("CONNECT result = %q %v hits=%d", body, err, hits.Load())
			}
		})
	}
}

func TestExplicitProxyValidation(t *testing.T) {
	for _, value := range []string{
		"127.0.0.1:7890", "ftp://localhost:7890", "socks4://localhost:1080", "http://", "http://localhost:0",
		"http://user:secret@host:invalid", "http://host/path", "http://host?query", "http://host#fragment",
	} {
		if unexpected, err := httpclient.NewClient(httpclient.Options{ProxyURL: value}); err == nil {
			_ = unexpected.Close()
			t.Errorf("invalid proxy accepted: %q", value)
		} else if strings.Contains(err.Error(), "secret") {
			t.Error("proxy password appeared in validation error")
		}
	}
	for _, options := range []httpclient.Options{
		{ProxyURL: "http://localhost:7890", NoProxy: true},
		{ProxyURL: "http://localhost:7890", TLSBackendFallback: true},
		{NoProxy: true, TLSBackendFallback: true},
	} {
		if unexpected, err := httpclient.NewTransport(options); err == nil {
			_ = unexpected.Close()
			t.Error("unsupported proxy options accepted")
		}
	}
}

func TestExplicitSOCKS5Proxy(t *testing.T) {
	for _, remoteDNS := range []bool{false, true} {
		t.Run(fmt.Sprintf("remoteDNS=%t", remoteDNS), func(t *testing.T) {
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "SOCKS echo") }))
			defer origin.Close()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan error, 1)
			go func() { done <- serveSOCKS5(listener, strings.TrimPrefix(origin.URL, "http://"), remoteDNS) }()
			scheme := "socks5"
			target := origin.URL
			if remoteDNS {
				scheme = "socks5h"
				_, port, _ := net.SplitHostPort(strings.TrimPrefix(origin.URL, "http://"))
				target = "http://unresolvable.invalid:" + port
			}
			proxyURL := (&url.URL{Scheme: scheme, Host: listener.Addr().String(), User: url.UserPassword("user", "pass")}).String()
			t.Setenv("NO_PROXY", "*")
			client := testClient(t, httpclient.Options{ProxyURL: proxyURL, Timeout: 5 * time.Second})
			response, err := client.Get(context.Background(), target)
			if err != nil {
				t.Fatal(err)
			}
			if body := string(readBody(t, response)); body != "SOCKS echo" {
				t.Fatal(body)
			}
			_ = client.Close()
			if err := await(t, done); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func serveSOCKS5(listener net.Listener, target string, remoteDNS bool) error {
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
	methods := make([]byte, int(greeting[1]))
	if _, err = io.ReadFull(conn, methods); err != nil {
		return err
	}
	if greeting[0] != 5 {
		return fmt.Errorf("invalid SOCKS version")
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
	var length [1]byte
	if _, err = io.ReadFull(conn, length[:]); err != nil {
		return err
	}
	pass := make([]byte, int(length[0]))
	if _, err = io.ReadFull(conn, pass); err != nil {
		return err
	}
	if auth[0] != 1 || string(user) != "user" || string(pass) != "pass" {
		return fmt.Errorf("invalid SOCKS authentication")
	}
	if _, err = conn.Write([]byte{1, 0}); err != nil {
		return err
	}
	var request [4]byte
	if _, err = io.ReadFull(conn, request[:]); err != nil {
		return err
	}
	if request[0] != 5 || request[1] != 1 {
		return fmt.Errorf("invalid SOCKS CONNECT")
	}
	var host string
	switch request[3] {
	case 1:
		ip := make([]byte, 4)
		if _, err = io.ReadFull(conn, ip); err != nil {
			return err
		}
		host = net.IP(ip).String()
	case 3:
		if _, err = io.ReadFull(conn, length[:]); err != nil {
			return err
		}
		domain := make([]byte, int(length[0]))
		if _, err = io.ReadFull(conn, domain); err != nil {
			return err
		}
		host = string(domain)
	case 4:
		ip := make([]byte, 16)
		if _, err = io.ReadFull(conn, ip); err != nil {
			return err
		}
		host = net.IP(ip).String()
	default:
		return fmt.Errorf("invalid SOCKS address type")
	}
	var port [2]byte
	if _, err = io.ReadFull(conn, port[:]); err != nil {
		return err
	}
	wantHost, wantPort, _ := net.SplitHostPort(target)
	if remoteDNS {
		wantHost = "unresolvable.invalid"
	}
	if host != wantHost || fmt.Sprint(binary.BigEndian.Uint16(port[:])) != wantPort {
		return fmt.Errorf("SOCKS target=%s:%d", host, binary.BigEndian.Uint16(port[:]))
	}
	if remoteDNS && request[3] != 3 {
		return fmt.Errorf("socks5h resolved DNS locally")
	}
	upstream, err := net.DialTimeout("tcp", target, time.Second)
	if err != nil {
		return err
	}
	defer upstream.Close()
	if _, err = conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0}); err != nil {
		return err
	}
	finished := make(chan struct{})
	go func() { _, _ = io.Copy(upstream, conn); _ = upstream.Close(); close(finished) }()
	_, _ = io.Copy(conn, upstream)
	_ = conn.Close()
	<-finished
	return nil
}
