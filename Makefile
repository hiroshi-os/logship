.PHONY: test build cluster cluster-stop bench chaos

test:
	go test ./...

build:
	mkdir -p bin
	go build -o bin/logship ./cmd/logship
	go build -o bin/logship-cli ./cmd/logship-cli
	go build -o bin/logship-bench ./cmd/bench

cluster:
	./scripts/local-cluster.sh

cluster-stop:
	./scripts/local-cluster.sh stop

bench: build
	./bin/logship-bench --brokers 127.0.0.1:9092 --n 20000 --value-bytes 200 --acks 1 --out benches/last.json

chaos:
	./scripts/chaos-kill-broker.sh broker-2
