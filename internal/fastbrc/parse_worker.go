package fastbrc

import (
	"bytes"
	"unsafe"

	"github.com/zeebo/xxh3"
)

// byteHash returns the fnv1a hash of b
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

func byteHashBCE(b []byte) uint32 {
	const prime32 = uint32(16777619)
	hash := uint32(2166136261)

	var i int
	length := len(b)

	// unsafe, the compiler was convinced  it needed bound checks
	// saves ~200ns per call on i7-7700 and 100ns on ryzen 9700 (both ~10% per call)
	bp := unsafe.Pointer(unsafe.SliceData(b))

	// Process 4 bytes at a time
	for i = 0; i+3 <= length-1; i += 4 {
		hash ^= uint32(*(*byte)(unsafe.Add(bp, i)))
		hash *= prime32
		hash ^= uint32(*(*byte)(unsafe.Add(bp, i+1)))
		hash *= prime32
		hash ^= uint32(*(*byte)(unsafe.Add(bp, i+2)))
		hash *= prime32
		hash ^= uint32(*(*byte)(unsafe.Add(bp, i+3)))
		hash *= prime32
	}

	// Process remaining bytes
	for ; i <= length-1; i++ {
		hash ^= uint32(*(*byte)(unsafe.Add(bp, i)))
		hash *= prime32
	}

	return hash
}

// ParseFixedPoint16Unsafe parses input as a 1 decimal place float and
// represents it as an int16
// No check for overflow or invalid values.

// ////go:noinline
func ParseFixedPoint16Unsafe(input []byte) int16 {
	bp := unsafe.Pointer(unsafe.SliceData(input))

	i := len(input) - 1
	value := int16(*(*byte)(unsafe.Add(bp, i)) - '0')
	i -= 2 // skip last num + dot
	var mult int16 = 10

	for ; i > 0; i-- {
		value += mult * int16(*(*byte)(unsafe.Add(bp, i))-'0')
		mult *= 10
	}
	if *(*byte)(bp) == '-' {
		value = -value
	} else {
		value += mult * int16(*(*byte)(bp)-'0')
	}
	return value
}

type ChunkGetter interface {
	NextChunk() *[]byte
	ReleaseChunk(*[]byte)
}

func ParseWorker(chunker ChunkGetter) []StationInt16 {
	// stationTable := make([]StationInt16, 65535)
	stationTable := make([]StationInt16, 65537)
	stationTablePtr := unsafe.Pointer(unsafe.SliceData(stationTable))
	stationTableLen := len(stationTable)
	stationSize := unsafe.Sizeof(StationInt16{})
	for i := range stationTable {
		stationTable[i].Min = 32767
		stationTable[i].Max = -32767
	}

	for {
		chunkPtr := chunker.NextChunk()
		if chunkPtr == nil {
			break
		}

		startpos := 0
		chunkmaxpos := len(*chunkPtr) - 1
		for startpos <= chunkmaxpos {
			delim := bytes.IndexByte((*chunkPtr)[startpos:chunkmaxpos], ';')
			//if delim < 0 {
			//	log.Fatal("garbage input, ';' not found")
			//}

			// h := byteHashBCE((*chunkPtr)[startpos:startpos+delim]) % uint32(stationTableLen)
			h := xxh3.Hash(((*chunkPtr)[startpos : startpos+delim])) % uint64(stationTableLen)

			station := (*StationInt16)(unsafe.Add(stationTablePtr, h*uint64(stationSize)))
			if station.N == 0 {
				station.Name = bytes.Clone((*chunkPtr)[startpos : startpos+delim])
			}
			// enable to check if there are collisions :-)
			//if !bytes.Equal(station.Name, (*chunkPtr)[startpos:startpos+delim]) {
			//	panic("woupelai")
			//}

			startpos += delim + 1

			nl := bytes.IndexByte((*chunkPtr)[startpos:], '\n')
			//if nl < 0 {
			//	log.Fatal("garbage input, '\\n' not found")
			//}
			value := (*chunkPtr)[startpos : startpos+nl]
			startpos += nl + 1

			m := ParseFixedPoint16Unsafe(value)
			//if err != nil {
			//	log.Fatal(err)
			//}

			station.NewMeasurement(m)
		}

		chunker.ReleaseChunk(chunkPtr)
	}

	return stationTable
}
