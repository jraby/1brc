package fastbrc

import (
	"bytes"
	"log"
)

// byteHash returns the fnv1 hash of b
func byteHash(b []byte) uint32 {
	const prime32 = uint32(16777619)
	hash := uint32(2166136261)

	i := 0
	length := len(b)

	// Process 4 bytes at a time
	for ; i+3 < length; i += 4 {
		hash ^= uint32(b[i])
		hash *= prime32
		hash ^= uint32(b[i+1])
		hash *= prime32
		hash ^= uint32(b[i+2])
		hash *= prime32
		hash ^= uint32(b[i+3])
		hash *= prime32
	}

	// Process remaining bytes
	for ; i < length; i++ {
		hash ^= uint32(b[i])
		hash *= prime32
	}

	return hash
}

// ParseFixedPoint16Unsafe parses input as a 1 decimal place float and
// represents it as an int16
// No check for overflow or invalid values.
func ParseFixedPoint16Unsafe(input []byte) (int16, error) {
	var value int16
	var mult int16 = 1
	for i := len(input) - 1; i >= 0; i-- {
		if input[i] == '-' {
			value = -value
			continue
		}

		if input[i] != '.' {
			value += mult * int16(input[i]-'0')
			mult *= 10
		}
	}
	return value, nil
}

type ChunkGetter interface {
	NextChunk() *[]byte
	ReleaseChunk(*[]byte)
}

func ParseWorker(chunker ChunkGetter) []StationInt16 {
	stationTable := make([]StationInt16, 65535)
	for i := range stationTable {
		stationTable[i].Min = 32767
		stationTable[i].Max = -32767
	}

	for {
		chunkPtr := chunker.NextChunk()
		if chunkPtr == nil {
			break
		}

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

			// enable to check if there are collisions :-)
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

		chunker.ReleaseChunk(chunkPtr)
	}

	return stationTable
}
