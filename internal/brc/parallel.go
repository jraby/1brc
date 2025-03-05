package brc

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"runtime"
	"sort"
	"strings"
	"sync"
)

func Carotte(inputFile string) string {
	n := runtime.NumCPU()
	readers, err := NewMmapedSectionReaders(inputFile, n)
	if err != nil {
		log.Fatalf("NewMmapedSectionReaders: %s", err)
	}

	stationMaps := make([]map[string]*StationInt16, n)
	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			stationMaps[i] = ParallelReadSliceFixedInt16Unsafe(readers[i])
		}()
	}

	wg.Wait()
	mergedStations := make(map[string]*StationInt16, 2048)
	for i := range stationMaps {
		for k, v := range stationMaps[i] {
			merged, ok := mergedStations[k]
			if !ok {
				merged = &StationInt16{
					Min:   v.Min,
					Max:   v.Max,
					Total: v.Total,
					N:     v.N,
				}
				mergedStations[k] = merged
				continue
			}

			merged.Total += v.Total
			merged.N += v.N
			if v.Min < merged.Min {
				v.Min = merged.Min
			}
			if v.Max > merged.Max {
				v.Max = merged.Max
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
	return strings.Join(out, "")
}

func ParallelReadSliceFixedInt16Unsafe(input io.Reader) map[string]*StationInt16 {
	stations := make(map[string]*StationInt16, 2048)

	br := bufio.NewReaderSize(input, 64*1024)

	for {
		line, err := br.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("ReadSlice: %s", err)
		}

		fieldSepPos := bytes.IndexByte(line, ';')
		if fieldSepPos == -1 {
			log.Fatalf("invalid line: %s", string(line))
		}

		m, err := ParseFixedPoint16Unsafe(line[fieldSepPos+1 : len(line)-1])
		if err != nil {
			log.Fatal(err)
		}

		station, ok := stations[string(line[:fieldSepPos])]
		if ok {
			station.NewMeasurement(m)
		} else {
			stations[string(line[:fieldSepPos])] = &StationInt16{
				Min:   m,
				Max:   m,
				Total: int32(m),
				N:     1,
			}
		}
	}

	return stations
}
