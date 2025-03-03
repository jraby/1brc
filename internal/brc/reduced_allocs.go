package brc

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"unsafe"
)

func ReducedAllocsMmapReader(inputFile string) string {
	reader, err := NewMmapReader(inputFile)
	if err != nil {
		log.Fatal(err)
	}
	return ReducedAllocs(reader)
}

func ReducedAllocsBufferedReader(input io.Reader) string {
	reader := bufio.NewReaderSize(input, 4*1024*1024)
	return ReducedAllocs(reader)
}

func ReducedAllocs(input io.Reader) string {
	stations := make(map[string]*Station, 2048)
	scanner := bufio.NewScanner(input)

	for scanner.Scan() {
		// name;float.111
		line := scanner.Bytes()
		fieldSepPos := bytes.IndexByte(line, ';')
		if fieldSepPos == -1 {
			log.Fatalf("invalid line: %s", string(line))
		}

		value := unsafe.String(unsafe.SliceData(line[fieldSepPos+1:]), len(line)-fieldSepPos-1) // skip \n at end or line

		m, err := strconv.ParseFloat(value, 64)
		if err != nil {
			log.Fatal(err)
		}

		station, ok := stations[string(line[:fieldSepPos])]
		if ok {
			station.NewMeasurement(m)
		} else {
			stations[string(line[:fieldSepPos])] = NewStation(m)
		}
	}

	keys := make([]string, 0, len(stations))
	for k := range stations {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]string, 0, len(stations)+2)
	out = append(out, "{")
	for _, k := range keys {
		out = append(out, fmt.Sprintf("%s=%0.1f/%0.1f/%0.1f, ", k, stations[k].Min, stations[k].Total/float64(stations[k].N), stations[k].Max))
	}
	out = append(out, "}")
	return strings.Join(out, "")
}
