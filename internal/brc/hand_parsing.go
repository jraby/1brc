package brc

import (
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/exp/mmap"
)

type parserState int

const (
	parserStateName parserState = iota
	parserStateSeparator
	parserStateValue
	parserStateEOL
)

func HandParserMmap(inputFile string) string {
	mm, err := mmap.Open(inputFile)
	if err != nil {
		log.Fatal(err)
	}
	return HandParsing(mm)
}

func HandParsing(input io.ReaderAt) string {
	stations := make(map[string]*Station, 2048)

	// scanner := bufio.NewScanner(input)
	// scanner.Split(bufio.ScanBytes)

	state := parserStateName
	var offset int64
	var stateStartOffset int64
	name := make([]byte, 0, 256)
	valueB := make([]byte, 0, 256)
	b := make([]byte, 1)

	for {
		_, err := input.ReadAt(b, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		if state == parserStateEOL {
			state = parserStateName
			stateStartOffset = offset
		} else if state == parserStateName && b[0] == ';' {
			_, err = input.ReadAt(name[:offset-stateStartOffset], stateStartOffset)
			if err != nil {
				log.Fatalf("reading name failed: %s", err)
			}
			name = name[:offset-stateStartOffset]

			state = parserStateSeparator
			stateStartOffset = offset
		} else if state == parserStateSeparator && b[0] != ';' {
			state = parserStateValue
			stateStartOffset = offset
		} else if state == parserStateValue && b[0] == '\n' {
			_, err = input.ReadAt(valueB[:offset-stateStartOffset], stateStartOffset)
			if err != nil {
				log.Fatalf("reading valueB failed: %s", err)
			}
			valueB = valueB[:offset-1-stateStartOffset]

			value := unsafe.String(unsafe.SliceData(valueB), offset-1-stateStartOffset)
			m, err := strconv.ParseFloat(value, 64)
			if err != nil {
				log.Fatal(err)
			}

			station, ok := stations[string(name)]
			if ok {
				station.NewMeasurement(m)
			} else {
				stations[string(name)] = NewStation(m)
			}

			state = parserStateEOL
			stateStartOffset = offset
		}
		offset++
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
