baseline: bin/baseline
	diff <(./output2diffable.sh ./data/10m.txt.expect) <(bin/baseline -i data/10m.txt | ./output2diffable.sh /dev/stdin)

bin/baseline: cmd/baseline/*.go | bin
	go build -o bin/baseline ./cmd/baseline 
bin:
	install -d bin

