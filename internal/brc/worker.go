package brc

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
)

const chunksize = 256 * 1024

type chunkPool struct {
	p sync.Pool
}

func (cp *chunkPool) Get() *[]byte {
	b := cp.p.Get().(*[]byte)
	*b = (*b)[:0] // reset
	return b
}

func (cp *chunkPool) Put(b *[]byte) {
	cp.p.Put(b)
}

var ChunkPool = chunkPool{
	p: sync.Pool{
		New: func() any {
			b := make([]byte, 0, chunksize)
			return &b
		},
	},
}

func chunker(r io.Reader, nreaders int) <-chan *[]byte {
	ch := make(chan *[]byte, nreaders)

	go func() {
		// runtime.LockOSThread()
		leftovers := make([]byte, 0, 256)
		for {
			chunk := ChunkPool.Get()
			*chunk = append(*chunk, leftovers...) // leftovers at beginning of chunk
			currentReadStartPos := len(leftovers) // keep ref for calculations
			leftovers = leftovers[:0]             // reset
			*chunk = (*chunk)[:cap(*chunk)]       // extend to use all cap

			n, err := r.Read((*chunk)[currentReadStartPos:])
			// log.Printf("n: %d, err: %v, currentReadStartPos: %d", n, err, currentReadStartPos)
			if err != nil {
				if err == io.EOF {
					// we didn't read anything, got eof, if we had leftovers, push them out
					if currentReadStartPos > 0 {
						*chunk = (*chunk)[:currentReadStartPos]
						if (*chunk)[len(*chunk)-1] != '\n' {
							// last line might not have a \n, make sure it does
							*chunk = append(*chunk, '\n')
						}
						ch <- chunk
					}
					close(ch)
					return
				}
				log.Fatalf("Read: %s", err)
			}
			(*chunk) = (*chunk)[:currentReadStartPos+n] // chop at last read to avoid having to calculate it everytime

			lastnl := bytes.LastIndexByte(*chunk, '\n')
			if lastnl == -1 {
				// no \n and not EOF, keep reading
				leftovers = append(leftovers, (*chunk)...)
				// log.Printf("n: %d, err: %v, currentReadStartPos: %d, leftovers: %d", n, err, currentReadStartPos, len(leftovers))
				continue
			}

			if lastnl < currentReadStartPos+n-1 {
				leftovers = append(leftovers, (*chunk)[lastnl+1:]...)
			}

			*chunk = (*chunk)[:lastnl+1]
			ch <- chunk
		}
	}()
	return ch
}

func ParallelWorkerRunner(inputFile string, nworkers int, parser func(<-chan *[]byte) []StationInt16) string {
	f, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("Open: %s", err)
	}

	// reader := bufio.NewReaderSize(f, 1024*1024)
	chunkCh := chunker(f, nworkers)

	stationTables := make([][]StationInt16, nworkers)
	wg := sync.WaitGroup{}
	wg.Add(nworkers)
	for i := range nworkers {
		go func() {
			defer wg.Done()
			// runtime.LockOSThread()
			stationTables[i] = parser(chunkCh)
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

func ParallelChunkChannelFixedInt16UnsafeOpenAddr(chunkCh <-chan *[]byte) []StationInt16 {
	stationTable := make([]StationInt16, 65535)
	for i := range stationTable {
		stationTable[i].Min = 32767
		stationTable[i].Max = -32767
	}
	for chunkPtr := range chunkCh {
		chunk := *chunkPtr

		startpos := 0
		lenchunk := len(chunk)
		for startpos < lenchunk {
			delim := bytes.IndexByte(chunk[startpos:], ';')
			if delim < 0 {
				log.Fatal("garbage input, ';' not found")
			}

			name := chunk[startpos : startpos+delim]
			startpos += delim + 1

			h := byteHash(name) % uint32(len(stationTable))

			station := &stationTable[h]
			if station.N == 0 {
				station.Name = bytes.Clone(name)
			}
			//if !bytes.Equal(station.Name, name) {
			//	panic("woupelai")
			//}

			nl := bytes.IndexByte(chunk[startpos:], '\n')
			if nl < 0 {
				log.Fatal("garbage input, '\\n' not found")
			}
			value := chunk[startpos : startpos+nl]
			startpos += nl + 1

			m, err := ParseFixedPoint16Unsafe(value)
			if err != nil {
				log.Fatal(err)
			}

			station.NewMeasurement(m)
		}

		ChunkPool.Put(chunkPtr)
	}

	return stationTable
}
