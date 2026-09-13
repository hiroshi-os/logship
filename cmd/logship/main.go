package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/hiroshi-os/logship/internal/broker"
)

func main() {
	var (
		id         = flag.Int("id", envInt("LOGSHIP_ID", 1), "broker id")
		bind       = flag.String("bind", env("LOGSHIP_BIND", "0.0.0.0:9092"), "listen address")
		advertise  = flag.String("advertise", env("LOGSHIP_ADVERTISE", ""), "address peers and clients use")
		data       = flag.String("data", env("LOGSHIP_DATA", "./data"), "data directory")
		peers      = flag.String("peers", env("LOGSHIP_PEERS", ""), "id=host:port,... static membership")
		fsyncEvery = flag.Int("fsync-every", envInt("LOGSHIP_FSYNC_EVERY", 0), "fsync every N appends (0=OS cache + close)")
		segBytes   = flag.Int64("segment-bytes", envInt64("LOGSHIP_SEGMENT_BYTES", 1<<20), "max segment size")
		idxInt     = flag.Int64("index-interval", envInt64("LOGSHIP_INDEX_INTERVAL", 4096), "sparse index interval in bytes")
		minISR     = flag.Int("min-isr", envInt("LOGSHIP_MIN_ISR", 1), "minimum in-sync replicas")
	)
	flag.Parse()

	plist, err := broker.ParsePeers(*peers)
	if err != nil {
		log.Fatal(err)
	}
	cfg := broker.Config{
		ID:              *id,
		Bind:            *bind,
		Advertise:       *advertise,
		DataDir:         *data,
		Peers:           plist,
		FsyncEvery:      *fsyncEvery,
		MaxSegmentBytes: *segBytes,
		IndexInterval:   *idxInt,
		MinISR:          *minISR,
		SessionTimeout:  3 * time.Second,
		GroupSession:    10 * time.Second,
		RebalanceWait:   2 * time.Second,
		ReplicaLagTime:  5 * time.Second,
		ProduceTimeout:  5 * time.Second,
	}
	b, err := broker.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err := b.Start(); err != nil {
		log.Fatal(err)
	}
	log.Printf("logship broker id=%d listen=%s advertise=%s", cfg.ID, b.Addr(), cfg.Advertise)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Printf("logship broker id=%d shutting down", cfg.ID)
	_ = b.Close()
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

func envInt64(k string, def int64) int64 {
	if v := os.Getenv(k); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return n
		}
	}
	return def
}
