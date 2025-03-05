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
			// station.NewMeasurement(m)
			station.NewMeasurement2(m)
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

func ReadSliceStringHash(input io.Reader) string {
	stations, err := NewStringHashTable(8192) // enough buckets for ~3k entries with load factor <0.5
	if err != nil {
		log.Fatalf("NewStringHashTable: %s", err)
	}

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

		name := unsafe.String(unsafe.SliceData(line[:fieldSepPos]), fieldSepPos)
		station := stations.getOrCreate(name)
		station.NewMeasurement(m)
	}

	keys := make([]string, 0, len(stations.KnownEntries()))
	keys = append(keys, stations.KnownEntries()...)
	sort.Strings(keys)

	out := make([]string, 0, len(stations.KnownEntries())+2)
	out = append(out, "{")
	for i, k := range keys {
		station := stations.getOrCreate(k)
		if i == len(keys)-1 {
			out = append(out, fmt.Sprintf("%s=%0.1f/%0.1f/%0.1f", k, station.Min, station.Total/float64(station.N), station.Max))
		} else {
			out = append(out, fmt.Sprintf("%s=%0.1f/%0.1f/%0.1f, ", k, station.Min, station.Total/float64(station.N), station.Max))
		}
	}
	out = append(out, "}")
	return strings.Join(out, "")
}

func ReadSliceInt32(input io.Reader) string {
	stations := make(map[string]*StationInt, 2048)

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
			stations[string(line[:fieldSepPos])] = NewStationInt(m)
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
			out = append(out, fmt.Sprintf("%s=%0.1f/%0.1f/%0.1f", k, float32(stations[k].Min)/10, float32(stations[k].Total)/10/float32(stations[k].N), float32(stations[k].Max)/10))
		} else {
			out = append(out, fmt.Sprintf("%s=%0.1f/%0.1f/%0.1f, ", k, float32(stations[k].Min)/10, float32(stations[k].Total)/10/float32(stations[k].N), float32(stations[k].Max)/10))
		}
	}
	out = append(out, "}")
	return strings.Join(out, "")
}

func ReadSliceFixedInt16(input io.Reader) string {
	stations := make(map[string]*StationInt16, 2048)

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

		m, err := ParseFixedPoint16(line[fieldSepPos+1 : len(line)-1])
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

	keys := make([]string, 0, len(stations))
	for k := range stations {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]string, 0, len(stations)+2)
	out = append(out, "{")
	for i, k := range keys {
		if i == len(keys)-1 {
			out = append(out, fmt.Sprintf("%s=%d.%d/%d.%d/%d.%d", k,
				stations[k].Min/10, stations[k].Min%10,
				stations[k].Total/stations[k].N/10, stations[k].Total/stations[k].N%10,
				stations[k].Max/10, stations[k].Max%10,
			))
		} else {
			out = append(out, fmt.Sprintf("%s=%d.%d/%d.%d/%d.%d, ", k,
				stations[k].Min/10, stations[k].Min%10,
				stations[k].Total/stations[k].N/10, stations[k].Total/stations[k].N%10,
				stations[k].Max/10, stations[k].Max%10,
			))
		}
	}
	out = append(out, "}")
	return strings.Join(out, "")
}

func ReadSliceFixedInt16Unsafe(input io.Reader) string {
	stations := make(map[string]*StationInt16, 2048)

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

	keys := make([]string, 0, len(stations))
	for k := range stations {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]string, 0, len(stations)+2)
	out = append(out, "{")
	for i, k := range keys {
		station := stations[k]
		if i == len(keys)-1 {
			out = append(out, fmt.Sprintf("%s=%s", k, station.FancyPrint()))
		} else {
			out = append(out, fmt.Sprintf("%s=%s, ", k, station.FancyPrint()))
		}
	}
	out = append(out, "}")
	return strings.Join(out, "")
}

func ReadSliceStringHashFixedInt16Unsafe(input io.Reader) string {
	stations, err := NewStringHashTableInt16Stations(8192) // enough buckets for ~3k entries with load factor <0.5
	if err != nil {
		log.Fatalf("NewStringHashTable: %s", err)
	}

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

		name := unsafe.String(unsafe.SliceData(line[:fieldSepPos]), fieldSepPos)
		m, err := ParseFixedPoint16Unsafe(line[fieldSepPos+1 : len(line)-1])
		if err != nil {
			log.Fatal(err)
		}
		station := stations.getOrCreate(name)
		if station.N == 0 {
			station.N = 1
			station.Total = int32(m)
			station.Min = m
			station.Max = m
		} else {
			station.NewMeasurement(m)
		}
	}

	keys := make([]string, 0, len(stations.KnownEntries()))
	keys = append(keys, stations.KnownEntries()...)
	sort.Strings(keys)

	out := make([]string, 0, len(stations.KnownEntries())+2)
	out = append(out, "{")
	for i, k := range keys {
		station := stations.getOrCreate(k)
		if i == len(keys)-1 {
			out = append(out, fmt.Sprintf("%s=%s", k, station.FancyPrint()))
			//			out = append(out, fmt.Sprintf("%s=%d.%d/%d.%d/%d.%d", k,
			//				station.Min/10, station.Min%10,
			//				station.Total/station.N/10, station.Total/station.N%10,
			//				station.Max/10, station.Max%10,
			//			))
		} else {
			out = append(out, fmt.Sprintf("%s=%s,", k, station.FancyPrint()))
			//			out = append(out, fmt.Sprintf("%s=%d.%d/%d.%d/%d.%d, ", k,
			//				station.Min/10, station.Min%10,
			//				station.Total/station.N/10, station.Total/station.N%10,
			//				station.Max/10, station.Max%10,
			//))
		}
	}
	out = append(out, "}")
	return strings.Join(out, "")
}
