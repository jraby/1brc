build: bin/fastbrc bin/runner

PERF_STAT_E = task-clock:u,page-faults:u,instructions:u,cycles:u,branches:u,branch-misses:u,cache-misses,cache-references,L1-dcache-load-misses,L1-dcache-loads,L1-dcache-stores,LLC-load-misses
NPROC?= $(shell nproc)
CHANNEL_CAP?=$(NPROC)
CHUNKSIZE?=$(shell echo $$((2048*1024)))
run: build
	perf stat -e $(PERF_STAT_E) bin/fastbrc -f data/1b.txt -n $(NPROC) -channel-cap $(CHANNEL_CAP) -chunksize $(CHUNKSIZE)

BENCH_PATTERN?=10m
bench:
	go test -run XXX -bench $(BENCH_PATTERN) -benchtime 1s ./...

bin/fastbrc:  *.go internal/fastbrc/*.go | bin
	go build -o bin/fastbrc ./main.go

fastbrc: bin/fastbrc
	diff <(./output2diffable.sh ./data/10m.txt.expect) <(bin/fastbrc -n 8 -f data/10m.txt | ./output2diffable.sh /dev/stdin) || true

bin/runner:  cmd/runner/*.go internal/brc/*.go | bin
	go build -o bin/runner ./cmd/runner 

check-runner.%: bin/runner
	diff -u <(./output2diffable.sh ./data/10m.txt.expect) <(bin/runner -funcName $* -i data/10m.txt | ./output2diffable.sh /dev/stdin)

runner.%: bin/runner
	perf stat -e $(PERF_STAT_E) bin/runner -funcName $* -n $(NPROC) -i data/1b.txt 

baseline: bin/baseline
	diff <(./output2diffable.sh ./data/10m.txt.expect) <(bin/baseline -i data/10m.txt | ./output2diffable.sh /dev/stdin) || true

bin/baseline: cmd/baseline/*.go | bin
	go build -o bin/baseline ./cmd/baseline 

faster: bin/faster
	diff <(./output2diffable.sh ./data/10m.txt.expect) <(bin/faster -i data/10m.txt | ./output2diffable.sh /dev/stdin) || true
bin/faster: cmd/faster/*.go | bin
	go build -o bin/faster ./cmd/faster 

bin:
	install -d bin

