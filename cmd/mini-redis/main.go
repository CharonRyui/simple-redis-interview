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
	snapshotPath := flag.String("snapshot", "", "snapshot path (default: AOF path + .snap)")
	maxBytes := flag.Int64("max-storage-bytes", redis.DefaultMaxBytes, "logical storage limit in bytes (0 = unlimited)")
	flag.Parse()
	if *maxBytes < 0 {
		log.Fatal("max-storage-bytes must be nonnegative")
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	log.Printf("mini redis bound to %s; recovering AOF %s before serving", listener.Addr(), *aofPath)
	if err := redis.ServeWithOptions(ctx, listener, redis.Options{
		AOFPath: *aofPath, SnapshotPath: *snapshotPath, MaxBytes: *maxBytes,
	}); err != nil {
		log.Fatal(err)
	}
}
