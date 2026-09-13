package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/hiroshi-os/logship/internal/client"
	"github.com/hiroshi-os/logship/internal/protocol"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	brokers := env("LOGSHIP_BROKERS", "127.0.0.1:9092")
	switch os.Args[1] {
	case "topic":
		topicCmd(brokers, os.Args[2:])
	case "produce":
		produceCmd(brokers, os.Args[2:])
	case "fetch":
		fetchCmd(brokers, os.Args[2:])
	case "consume":
		consumeCmd(brokers, os.Args[2:])
	case "metadata":
		md, err := client.New(split(brokers)).Metadata()
		must(err)
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(md)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `logship-cli — HTTP client for a logship cluster

  logship-cli metadata
  logship-cli topic create <name> [--partitions 3] [--rf 3]
  logship-cli produce <topic> [--key k] --value v [--acks 1|all]
  logship-cli fetch <topic> [--partition 0] [--offset 0]
  logship-cli consume <topic> --group <id> [--from-beginning]

LOGSHIP_BROKERS (default 127.0.0.1:9092) is a comma-separated list.
`)
}

func topicCmd(brokers string, args []string) {
	if len(args) < 2 || args[0] != "create" {
		usage()
		os.Exit(2)
	}
	fs := flag.NewFlagSet("topic create", flag.ExitOnError)
	n := fs.Int("partitions", 3, "partition count")
	rf := fs.Int("rf", 3, "replication factor")
	_ = fs.Parse(args[2:])
	must(client.New(split(brokers)).CreateTopic(args[1], *n, *rf))
	fmt.Println("created", args[1])
}

func produceCmd(brokers string, args []string) {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	key := fs.String("key", "", "record key (empty = round-robin)")
	value := fs.String("value", "", "record value")
	acks := fs.String("acks", "1", "1 or all")
	topic, args := shiftTopic(args)
	_ = fs.Parse(args)
	if topic == "" {
		topic = fs.Arg(0)
	}
	if topic == "" || *value == "" {
		usage()
		os.Exit(2)
	}
	cli := client.New(split(brokers))
	resp, err := cli.Produce("", protocol.ProduceRequest{
		Topic: topic, Key: *key, Value: *value, Acks: *acks,
	})
	must(err)
	b, _ := json.MarshalIndent(resp, "", "  ")
	fmt.Println(string(b))
}

func fetchCmd(brokers string, args []string) {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	part := fs.Int("partition", 0, "partition")
	off := fs.Int64("offset", 0, "start offset")
	topic, args := shiftTopic(args)
	_ = fs.Parse(args)
	if topic == "" {
		topic = fs.Arg(0)
	}
	if topic == "" {
		usage()
		os.Exit(2)
	}
	fr, err := client.New(split(brokers)).Fetch("", topic, *part, *off, 1<<20, false)
	must(err)
	b, _ := json.MarshalIndent(fr, "", "  ")
	fmt.Println(string(b))
}

func consumeCmd(brokers string, args []string) {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	groupID := fs.String("group", "", "consumer group")
	fromBeg := fs.Bool("from-beginning", false, "start at 0 if no committed offset")
	max := fs.Int("max", 0, "exit after N records (0=run forever)")
	topic, args := shiftTopic(args)
	_ = fs.Parse(args)
	if topic == "" {
		topic = fs.Arg(0)
	}
	if topic == "" || *groupID == "" {
		usage()
		os.Exit(2)
	}
	cli := client.New(split(brokers))
	var member string
	var gen int
	var printed int
	for {
		jr, err := cli.Join(*groupID, protocol.JoinRequest{MemberID: member, Topics: []string{topic}})
		must(err)
		member, gen = jr.MemberID, jr.Generation
		sr, err := cli.Sync(*groupID, protocol.SyncRequest{MemberID: member, Generation: gen})
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		gen = sr.Generation
		parts := sr.Assignment[topic]
		offsets := map[int]int64{}
		of, err := cli.FetchOffsets(*groupID, topic)
		if err == nil {
			for _, o := range of.Offsets {
				offsets[o.Partition] = o.Offset
			}
		}
		for _, p := range parts {
			if _, ok := offsets[p]; !ok {
				if *fromBeg {
					offsets[p] = 0
				} else {
					// start at HW (latest) by fetching metadata
					md, err := cli.Metadata()
					offsets[p] = 0
					if err == nil {
						for _, t := range md.Topics {
							if t.Name != topic {
								continue
							}
							for _, pi := range t.Partitions {
								if pi.ID == p {
									offsets[p] = pi.HW
								}
							}
						}
					}
				}
			}
		}
		idle := 0
		for {
			hb, err := cli.Heartbeat(*groupID, protocol.HeartbeatRequest{MemberID: member, Generation: gen})
			if err != nil || hb.Error != "" {
				break
			}
			progress := false
			for _, p := range parts {
				fr, err := cli.Fetch("", topic, p, offsets[p], 1<<20, false)
				if err != nil || len(fr.Records) == 0 {
					continue
				}
				progress = true
				for _, rec := range fr.Records {
					fmt.Printf("p=%d offset=%d key=%s value=%s\n", p, rec.Offset, rec.Key, rec.Value)
					offsets[p] = rec.Offset + 1
					printed++
					if *max > 0 && printed >= *max {
						_ = cli.CommitOffsets(*groupID, protocol.OffsetCommitRequest{
							MemberID: member, Generation: gen,
							Offsets: []protocol.OffsetCommit{{Topic: topic, Partition: p, Offset: offsets[p]}},
						})
						return
					}
				}
				_ = cli.CommitOffsets(*groupID, protocol.OffsetCommitRequest{
					MemberID: member, Generation: gen,
					Offsets: []protocol.OffsetCommit{{Topic: topic, Partition: p, Offset: offsets[p]}},
				})
			}
			if !progress {
				idle++
				time.Sleep(200 * time.Millisecond)
				if *max > 0 && idle > 25 {
					return
				}
			}
		}
	}
}

func shiftTopic(args []string) (string, []string) {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		return args[0], args[1:]
	}
	return "", args
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

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
