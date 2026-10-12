package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"mini-redis/internal/redis"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "TCP listen address")
	aofPath := flag.String("aof", "appendonly.aof", "persistent log path (reuse the same file on restart)")
	flag.Parse()
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	log.Printf("mini redis bound to %s; recovering AOF %s before serving", listener.Addr(), *aofPath)
	if err := redis.Serve(ctx, listener, *aofPath); err != nil {
		log.Fatal(err)
	}
}
