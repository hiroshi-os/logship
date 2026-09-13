// Command bench measures produce and consume throughput against a running cluster.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hiroshi-os/logship/internal/client"
	"github.com/hiroshi-os/logship/internal/protocol"
)

func main() {
	brokers := flag.String("brokers", env("LOGSHIP_BROKERS", "127.0.0.1:9092"), "comma-separated brokers")
	topic := flag.String("topic", "bench", "topic name")
	n := flag.Int("n", 20000, "records to produce / consume")
	valueBytes := flag.Int("value-bytes", 200, "payload size")
	partitions := flag.Int("partitions", 3, "topic partitions")
	rf := flag.Int("rf", 3, "replication factor")
	acks := flag.String("acks", "1", "produce acks")
	workers := flag.Int("workers", 4, "concurrent produce workers")
	keyed := flag.Bool("keyed", true, "use keys (hash routing)")
	out := flag.String("out", "", "optional JSON results path")
	flag.Parse()

	cli := client.New(split(*brokers))
	if err := cli.CreateTopic(*topic, *partitions, *rf); err != nil {
		fmt.Fprintf(os.Stderr, "create topic (ok if exists): %v\n", err)
	}
	// wait briefly for replica assignment to propagate
	time.Sleep(400 * time.Millisecond)

	payload := strings.Repeat("x", *valueBytes)
	var produced atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	per := *n / *workers
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := client.New(split(*brokers))
			count := per
			if w == *workers-1 {
				count = *n - per*(*workers-1)
			}
			for i := 0; i < count; i++ {
				key := ""
				if *keyed {
					key = fmt.Sprintf("k-%d-%d", w, i)
				}
				_, err := c.Produce("", protocol.ProduceRequest{
					Topic: *topic, Key: key, Value: payload, Acks: *acks,
				})
				if err != nil {
					fmt.Fprintf(os.Stderr, "produce: %v\n", err)
					return
				}
				produced.Add(1)
			}
		}(w)
	}
	wg.Wait()
	prodDur := time.Since(start)
	prodN := produced.Load()
	prodOps := float64(prodN) / prodDur.Seconds()

	// Consume by scanning every partition from 0 to HW.
	md, err := cli.Metadata()
	if err != nil {
		fmt.Fprintf(os.Stderr, "metadata: %v\n", err)
		os.Exit(1)
	}
	var parts []int
	for _, t := range md.Topics {
		if t.Name == *topic {
			for _, p := range t.Partitions {
				parts = append(parts, p.ID)
			}
		}
	}
	start = time.Now()
	var consumed int64
	offs := make(map[int]int64, len(parts))
	deadline := time.Now().Add(15 * time.Second)
	for consumed < prodN && time.Now().Before(deadline) {
		progress := false
		for _, p := range parts {
			fr, err := cli.Fetch("", *topic, p, offs[p], 1<<20, false)
			if err != nil {
				fmt.Fprintf(os.Stderr, "fetch p=%d: %v\n", p, err)
				continue
			}
			if len(fr.Records) == 0 {
				continue
			}
			consumed += int64(len(fr.Records))
			offs[p] = fr.Records[len(fr.Records)-1].Offset + 1
			progress = true
		}
		if !progress {
			time.Sleep(40 * time.Millisecond)
		}
	}
	consDur := time.Since(start)
	consOps := float64(0)
	if consDur > 0 {
		consOps = float64(consumed) / consDur.Seconds()
	}

	result := map[string]any{
		"topic":             *topic,
		"acks":              *acks,
		"keyed":             *keyed,
		"value_bytes":       *valueBytes,
		"workers":           *workers,
		"produced":          prodN,
		"produce_seconds":   prodDur.Seconds(),
		"produce_ops_per_s": prodOps,
		"consumed":          consumed,
		"consume_seconds":   consDur.Seconds(),
		"consume_ops_per_s": consOps,
		"measured_at":       time.Now().UTC().Format(time.RFC3339),
		"note":              "measured against a live cluster; not estimated",
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(result)
	if *out != "" {
		b, _ := json.MarshalIndent(result, "", "  ")
		_ = os.WriteFile(*out, b, 0o644)
	}
	fmt.Fprintf(os.Stderr, "produce %.0f ops/s (%d in %s)  consume %.0f ops/s (%d in %s)\n",
		prodOps, prodN, prodDur.Truncate(time.Millisecond), consOps, consumed, consDur.Truncate(time.Millisecond))
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
