package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/csbxd/gocodex/httpclient"
)

func main() {
	url := flag.String("url", "", "HTTP or HTTPS URL to request")
	proxy := flag.String("proxy", "", "HTTP, HTTPS, SOCKS5 or SOCKS5h proxy URL")
	noProxy := flag.Bool("no-proxy", false, "connect directly, ignoring system and environment proxies")
	flag.Parse()
	if *url == "" {
		flag.Usage()
		return
	}
	if err := run(*url, *proxy, *noProxy); err != nil {
		log.Fatal(err)
	}
}

func run(url, proxy string, noProxy bool) error {
	transport, err := httpclient.NewTransport(httpclient.Options{
		UserAgent: "gocodex-roundtripper-example",
		ProxyURL:  proxy,
		NoProxy:   noProxy,
	})
	if err != nil {
		return err
	}
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	response, err := client.Get(url)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	fmt.Printf("%s %s\n", response.Proto, response.Status)
	_, err = io.Copy(log.Writer(), response.Body)
	return err
}
