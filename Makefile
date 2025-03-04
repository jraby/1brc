bench:
	go test -run XXX -bench . -benchtime 5s ./...
run: runner
#	/bin/time -p bin/baseline -i data/10m.txt
#	/bin/time -p bin/faster -i data/10m.txt

bin/runner:  cmd/runner/*.go internal/brc/*.go | bin
	go build -o bin/runner ./cmd/runner 

runner.%: bin/runner
	diff -u <(./output2diffable.sh ./data/10m.txt.expect) <(bin/runner -funcName $* -i data/10m.txt | ./output2diffable.sh /dev/stdin)

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

