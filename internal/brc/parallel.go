package brc

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"runtime"
	"slices"
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
			runtime.LockOSThread()
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

func ParallelReadSliceFixedInt16UnsafeBSearchNames(inputFile string) string {
	n := runtime.NumCPU()
	readers, err := NewMmapedSectionReaders(inputFile, n)
	if err != nil {
		log.Fatalf("NewMmapedSectionReaders: %s", err)
	}

	stationTables := make([][]StationInt16, n)
	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			runtime.LockOSThread()
			stationTables[i] = parallelReadSliceFixedInt16UnsafeBSearchNames(readers[i])
		}()
	}

	wg.Wait()
	mergedStations := make(map[string]*StationInt16, 2048)
	for i := range stationTables {
		table := stationTables[i]
		for j := range table {
			merged, ok := mergedStations[string(table[j].Name)]
			if !ok {
				merged = &StationInt16{
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
	return strings.Join(out, "")
}

func parallelReadSliceFixedInt16UnsafeBSearchNames(input io.Reader) []StationInt16 {
	stationTable := make([]StationInt16, 0, 2048)
	stationIndexFromTable := func(name []byte) int {
		// log.Printf("searching: %32s, %v", string(name), name)
		i := sort.Search(len(stationTable), func(i int) bool {
			return bytes.Compare(stationTable[i].Name, name) >= 0
		})

		if i < len(stationTable) && bytes.Equal(stationTable[i].Name, name) {
			return i
		}

		// log.Printf("not found: %32s, %v", string(name), name)
		// not found
		stationTable = append(stationTable, StationInt16{Name: bytes.Clone(name)})
		slices.SortFunc(stationTable, func(i, j StationInt16) int {
			return bytes.Compare(i.Name, j.Name)
		})

		i = sort.Search(len(stationTable), func(i int) bool {
			return bytes.Compare(stationTable[i].Name, name) >= 0
		})

		if i < len(stationTable) && bytes.Equal(stationTable[i].Name, name) {
			return i
		}

		// log.Printf("idx: %d, len: %d", i, len(stationTable))
		// log.Printf("name: %32s, %v", string(name), name)
		// log.Printf("table: %+v", stationTable)
		panic("well that ain't good")
	}

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

		station := &stationTable[stationIndexFromTable(line[:fieldSepPos])]
		if station.N > 0 {
			station.NewMeasurement(m)
		} else {
			station.Min = m
			station.Max = m
			station.Total = int32(m)
			station.N = 1
		}
	}

	return stationTable
}

func ParallelReadSliceFixedInt16UnsafeOpenAddr(inputFile string) string {
	n := runtime.NumCPU()
	// n := 1
	readers, err := NewMmapedSectionReaders(inputFile, n)
	// readers, err := NewMmapedSectionReadersMadv(inputFile, n)
	if err != nil {
		log.Fatalf("NewMmapedSectionReaders: %s", err)
	}

	stationTables := make([][]StationInt16, n)
	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			runtime.LockOSThread()
			stationTables[i] = parallelReadSliceFixedInt16UnsafeOpenAddr(readers[i])
		}()
	}

	wg.Wait()
	mergedStations := make(map[string]*StationInt16, 2048)
	for i := range stationTables {
		table := stationTables[i]
		for j := range table {
			if len(table[j].Name) == 0 {
				continue
			}
			merged, ok := mergedStations[string(table[j].Name)]
			if !ok {
				merged = &StationInt16{
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
	return strings.Join(out, "")
}

func ParallelRunner(inputFile string, nworkers int, parser func(io.Reader) []StationInt16) string {
	readers, err := NewMmapedSectionReaders(inputFile, nworkers)
	if err != nil {
		log.Fatalf("NewMmapedSectionReaders: %s", err)
	}

	stationTables := make([][]StationInt16, nworkers)
	wg := sync.WaitGroup{}
	wg.Add(nworkers)
	for i := range nworkers {
		go func() {
			defer wg.Done()
			runtime.LockOSThread()
			stationTables[i] = parser(readers[i])
		}()
	}

	wg.Wait()
	mergedStations := make(map[string]*StationInt16, 2048)
	for i := range stationTables {
		table := stationTables[i]
		for j := range table {
			if len(table[j].Name) == 0 {
				continue
			}
			merged, ok := mergedStations[string(table[j].Name)]
			if !ok {
				merged = &StationInt16{
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
	return strings.Join(out, "")
}

func parallelReadSliceFixedInt16UnsafeOpenAddr(input io.Reader) []StationInt16 {
	stationTable := make([]StationInt16, 65535)
	// hasher := fnv.New64a()
	// hasher := murmur3.New64()
	// hasher := crc64.New(crc64.MakeTable(crc64.ECMA))
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

		// h := xxhash.Sum64(line[:fieldSepPos]) % uint64(len(stationTable))
		// hasher.Reset()
		// hasher.Write(line[:fieldSepPos])
		// h := hasher.Sum64() % uint64(len(stationTable))

		h := byteHash(line[:fieldSepPos]) % uint32(len(stationTable))

		station := &stationTable[h]
		if station.N > 0 {
			//if !bytes.Equal(station.Name, line[:fieldSepPos]) {
			//	panic("woupelai")
			//}
			station.NewMeasurement(m)
		} else {
			station.Name = bytes.Clone(line[:fieldSepPos])
			station.Min = m
			station.Max = m
			station.Total = int32(m)
			station.N = 1
		}
	}

	return stationTable
}

func ParallelReadSlicePatateLineFixedInt16UnsafeOpenAddr(input io.Reader) []StationInt16 {
	stationTable := make([]StationInt16, 65535)
	for i := range stationTable {
		stationTable[i].Min = 32767
		stationTable[i].Max = -32767
	}
	br := bufio.NewReaderSize(input, 64*1024)

	for {
		name, err := br.ReadSlice(';')
		if err != nil {
			log.Fatalf("ReadSlice ';' : %s", err)
		}
		name = name[:len(name)-1]
		h := byteHash(name) % uint32(len(stationTable))

		station := &stationTable[h]
		if station.N == 0 {
			station.Name = bytes.Clone(name)
		}
		//if !bytes.Equal(station.Name, name) {
		//	panic("woupelai")
		//}

		value, err := br.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("ReadSlice: %s", err)
		}
		value = value[:len(value)-1]

		m, err := ParseFixedPoint16Unsafe(value)
		if err != nil {
			log.Fatal(err)
		}

		station.NewMeasurement(m)
		// station.NewMeasurementNoBranch(m)
	}

	return stationTable
}
