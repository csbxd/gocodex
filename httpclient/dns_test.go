//go:build cgo && linux && (amd64 || arm64)

package httpclient_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	codexhttp "github.com/csbxd/gocodex/httpclient"
)

func TestDNSFailurePreservesTransportTypes(t *testing.T) {
	// Both the native and Go resolvers reject this label without external DNS.
	host := strings.Repeat("n", 64) + ".invalid"
	for _, tc := range []struct {
		name, url string
		options   codexhttp.Options
	}{
		{"automatic", "http://" + host + "/?token=private-token", codexhttp.Options{}},
		{"direct", "http://" + host + "/?token=private-token", codexhttp.Options{NoProxy: true}},
		{"proxy", "http://127.0.0.1:1/", codexhttp.Options{ProxyURL: "http://" + host + ":8080"}},
		{"SOCKS local lookup", "http://" + host + "/", codexhttp.Options{ProxyURL: "socks5://127.0.0.1:1"}},
	} {
		for _, api := range []string{"client", "transport"} {
			t.Run(tc.name+"/"+api, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				var failure error
				if api == "client" {
					client := testClient(t, tc.options)
					_, failure = client.Get(ctx, tc.url)
				} else {
					transport, err := codexhttp.NewTransport(tc.options)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = transport.Close() })
					request, err := http.NewRequestWithContext(ctx, http.MethodGet, tc.url, nil)
					if err != nil {
						t.Fatal(err)
					}
					_, failure = transport.RoundTrip(request)
				}
				var op *net.OpError
				var dns *net.DNSError
				var native *codexhttp.Error
				if ctx.Err() != nil || !errors.As(failure, &op) || !errors.As(failure, &dns) || !errors.As(failure, &native) || native.Kind != "dns" {
					t.Fatalf("DNS classification lost: %T %v (context: %v)", failure, failure, ctx.Err())
				}
				if op.Op != "dial" || strings.Contains(native.Message, "private-token") {
					t.Fatalf("incorrect operation or URL disclosure: %v", failure)
				}
				if dns.Name != "" || dns.Server != "" || dns.IsNotFound || dns.IsTemporary {
					t.Fatalf("invented unavailable resolver metadata: %+v", dns)
				}
			})
		}
	}
}

func TestDNSClassificationDoesNotReplaceOtherFailures(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, "dns error: Name or service not known")
	}))
	t.Cleanup(server.Close)
	client := testClient(t, codexhttp.Options{NoProxy: true})
	_, failure := client.Get(t.Context(), server.URL)
	var native *codexhttp.Error
	var op *net.OpError
	var dns *net.DNSError
	if !errors.As(failure, &native) || errors.As(failure, &op) || errors.As(failure, &dns) {
		t.Fatalf("certificate failure was classified as DNS: %T %v", failure, failure)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, failure = client.Get(ctx, "http://"+strings.Repeat("n", 64)+".invalid/")
	if !errors.Is(failure, context.Canceled) || errors.As(failure, &dns) {
		t.Fatalf("caller cancellation was replaced: %v", failure)
	}
	plain := httptest.NewServer(server.Config.Handler)
	t.Cleanup(plain.Close)
	response, err := client.Get(t.Context(), plain.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusUnauthorized || string(data) != "dns error: Name or service not known" {
		t.Fatalf("HTTP status/body was reclassified: status=%d body=%q err=%v", response.StatusCode, data, err)
	}
}
