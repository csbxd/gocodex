package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	codexws "github.com/csbxd/gocodex/websocket"
)

func main() {
	url := flag.String("url", "", "ws or wss echo server URL")
	message := flag.String("message", "hello from Go via Codex", "text to send")
	flag.Parse()
	if *url == "" {
		flag.Usage()
		return
	}
	if err := run(*url, *message); err != nil {
		log.Fatal(err)
	}
}

func run(url, message string) error {
	client, err := codexws.NewClient(codexws.Options{HandshakeTimeout: 10 * time.Second})
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, handshake, err := client.Dial(ctx, url, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	fmt.Printf("handshake: %d\n", handshake.StatusCode)
	if err = conn.WriteMessage(ctx, codexws.TextMessage, []byte(message)); err != nil {
		return err
	}
	for {
		kind, data, err := conn.ReadMessage(ctx)
		if err != nil {
			return err
		}
		if kind == codexws.TextMessage || kind == codexws.BinaryMessage {
			fmt.Printf("echo: %s\n", data)
			return nil
		}
	}
}
