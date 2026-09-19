package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"time"

	codexhttp "github.com/csbxd/gocodex/httpclient"
)

func main() {
	url := flag.String("url", "", "HTTP or HTTPS URL to request")
	flag.Parse()
	if *url == "" {
		flag.Usage()
		return
	}
	if err := run(*url); err != nil {
		log.Fatal(err)
	}
}

func run(url string) error {
	client, err := codexhttp.NewClient(codexhttp.Options{UserAgent: "gocodex-http-example"})
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.Get(ctx, url)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	fmt.Printf("%s %d\n", response.Protocol, response.StatusCode)
	_, err = io.Copy(log.Writer(), response.Body)
	return err
}
