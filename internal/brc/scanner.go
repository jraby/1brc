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

func PatateMmapReader(inputFile string) string {
	reader, err := NewMmapReader(inputFile)
	if err != nil {
		log.Fatal(err)
	}
	return Patate(reader)
}

func PatateBufferedReader(input io.Reader) string {
	reader := bufio.NewReaderSize(input, 4*1024*1024)
	return Patate(reader)
}

type Splitter struct {
	State parserState
}

func (s *Splitter) Split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	var i int
	if s.State == parserStateName {
		i = bytes.IndexByte(data, ';')
	} else {
		i = bytes.IndexByte(data, '\n')
	}
	if i == -1 {
		if !atEOF {
			return 0, nil, nil
		}
		if len(data) > 0 {
			// If we have reached the end, return the last token.
			return 0, data, bufio.ErrFinalToken
		} else {
			return 0, nil, bufio.ErrFinalToken
		}
	}

	// return token without delimiter
	return i + 1, data[:i], nil
}

func Patate(input io.Reader) string {
	stations := make(map[string]*Station, 2048)
	scanner := bufio.NewScanner(input)

	// state := parserStateName
	var station *Station
	splitter := Splitter{State: parserStateName}

	// splitter := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
	//	var i int
	//	if state == parserStateName {
	//		i = bytes.IndexByte(data, ';')
	//	} else {
	//		i = bytes.IndexByte(data, '\n')
	//	}
	//	if i == -1 {
	//		if !atEOF {
	//			return 0, nil, nil
	//		}
	//		if len(data) > 0 {
	//			// If we have reached the end, return the last token.
	//			return 0, data, bufio.ErrFinalToken
	//		} else {
	//			return 0, nil, bufio.ErrFinalToken
	//		}
	//	}

	//	// return token without delimiter
	//	return i + 1, data[:i], nil
	//}

	scanner.Split(splitter.Split)
	for scanner.Scan() {
		switch splitter.State {
		case parserStateName:
			s, ok := stations[string(scanner.Bytes())]
			if !ok {
				s = &Station{}
				stations[string(scanner.Bytes())] = s
			}
			station = s
			splitter.State = parserStateValue
		case parserStateValue:
			b := scanner.Bytes()
			value := unsafe.String(unsafe.SliceData(b), len(b))
			m, err := strconv.ParseFloat(value, 64)
			if err != nil {
				log.Fatal(err)
			}
			station.NewMeasurement(m)
			splitter.State = parserStateName
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
