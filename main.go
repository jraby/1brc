package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"

	"1brc/internal/fastbrc"
)

func main() {
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	nworkers := flag.Int("n", 1, "number of workers for parallel funcs")
	chunkSize := flag.Int("chunksize", 256*1024, "size of the chunks to be processed by workers")
	chunkerChannelCap := flag.Int("channel-cap", 256, "capacity of the chunk channel")
	inputFile := flag.String("f", "data/10m.txt", "input file")
	var loglevel slog.Level
	flag.TextVar(&loglevel, "loglevel", slog.LevelInfo, "loglevel")

	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: loglevel,
	})))

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	f, err := os.Open(*inputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	chunker := fastbrc.NewChunker(f, *chunkerChannelCap, *chunkSize)

	stationTables := make([][]fastbrc.StationInt16, *nworkers)
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := chunker.Run(); err != nil {
			log.Fatalf("chunker failed: %s", err)
		}
		slog.Debug("Chunker done")
	}()

	wg.Add(*nworkers)
	for i := range *nworkers {
		go func() {
			defer wg.Done()
			stationTables[i] = fastbrc.ParseWorker(chunker)
			slog.Debug("Worker done", "id", i)
		}()
	}

	wg.Wait()

	mergedStations := make(map[string]*fastbrc.StationInt16, 2048)
	for i := range stationTables {
		table := stationTables[i]
		for j := range table {
			if len(table[j].Name) == 0 {
				continue
			}
			merged, ok := mergedStations[string(table[j].Name)]
			if !ok {
				merged = &fastbrc.StationInt16{
					Min:   table[j].Min,
					Max:   table[j].Max,
					Total: table[j].Total,
					N:     table[j].N,
				}
				mergedStations[string(table[j].Name)] = merged
				continue
			}

			merged.Total += table[j].Total
			merged.N += table[j].N
			if table[j].Min < merged.Min {
				table[j].Min = merged.Min
			}
			if table[j].Max > merged.Max {
				table[j].Max = merged.Max
			}
		}
	}

	keys := make([]string, 0, len(mergedStations))
	for k := range mergedStations {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]string, 0, len(mergedStations)+2)
	out = append(out, "{")
	for i, k := range keys {
		station := mergedStations[k]
		if i == len(keys)-1 {
			out = append(out, fmt.Sprintf("%s=%s", k, station.FancyPrint()))
		} else {
			out = append(out, fmt.Sprintf("%s=%s, ", k, station.FancyPrint()))
		}
	}
	out = append(out, "}")
	fmt.Println(strings.Join(out, ""))
	slog.Debug("all done")
}
