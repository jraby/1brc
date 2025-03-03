package brc

import (
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"unsafe"
)

type parserState int

const (
	parserStateName parserState = iota
	parserStateSeparator
	parserStateValue
	parserStateEOL
)

func HandParserMmap(inputFile string) string {
	reader, err := NewMmapReader(inputFile)
	if err != nil {
		log.Fatal(err)
	}
	return HandParsing(reader)
}

func HandParsing(input io.Reader) string {
	stations := make(map[string]*Station, 2048)

	state := parserStateName
	var offset int64
	name := make([]byte, 0, 256)
	valueB := make([]byte, 0, 256)
	b := make([]byte, 1)

	for {
		_, err := input.Read(b)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		if state == parserStateEOL && b[0] != '\n' {
			name = name[:0]
			name = append(name, b[0])
			state = parserStateName
		} else if state == parserStateName && b[0] != ';' {
			name = append(name, b[0])
		} else if state == parserStateName && b[0] == ';' {
			state = parserStateSeparator
		} else if state == parserStateSeparator && b[0] != ';' {
			state = parserStateValue
			valueB = valueB[:0]
			valueB = append(valueB, b[0])
		} else if state == parserStateValue && b[0] != '\n' {
			valueB = append(valueB, b[0])
		} else if state == parserStateValue && b[0] == '\n' {
			value := unsafe.String(unsafe.SliceData(valueB), len(valueB))
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
