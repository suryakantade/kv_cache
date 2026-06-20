package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/skantade/kv-cache/commands"
	"github.com/skantade/kv-cache/server"
	"github.com/skantade/kv-cache/store"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:6380", "listen address")
	maxKeys := flag.Int("maxkeys", 0, "max keys (0 = unlimited, uses LRU eviction when full)")
	maxConns := flag.Int("maxconns", 0, "max concurrent connections (0 = unlimited)")
	flag.Parse()

	kv := store.New(*maxKeys)
	defer kv.Close()

	handler := commands.NewHandler(kv)

	srv := server.New(server.Config{
		Addr:        *addr,
		MaxConns:    *maxConns,
		ReadBufSize: 4096,
	}, handler)

	// Graceful shutdown on SIGINT / SIGTERM.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		log.Println("shutting down...")
		if err := srv.Shutdown(context.Background()); err != nil {
			log.Printf("shutdown error: %v", err)
		}
	}()

	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
