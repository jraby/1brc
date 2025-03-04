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

func ReadSliceMmap(inputFile string) string {
	mr, err := NewMmapReader(inputFile)
	if err != nil {
		log.Fatal(err)
	}
	return ReadSlice(mr)
}

func ReadSlice(input io.Reader) string {
	stations := make(map[string]*Station, 2048)

	br := bufio.NewReaderSize(input, 1024*1024)

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

		value := unsafe.String(unsafe.SliceData(line[fieldSepPos+1:]), len(line)-fieldSepPos-2) // skip \n at end or line

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
	for i, k := range keys {
		if i == len(keys)-1 {
			out = append(out, fmt.Sprintf("%s=%0.1f/%0.1f/%0.1f", k, stations[k].Min, stations[k].Total/float64(stations[k].N), stations[k].Max))
		} else {
			out = append(out, fmt.Sprintf("%s=%0.1f/%0.1f/%0.1f, ", k, stations[k].Min, stations[k].Total/float64(stations[k].N), stations[k].Max))
		}
	}
	out = append(out, "}")
	return strings.Join(out, "")
}
