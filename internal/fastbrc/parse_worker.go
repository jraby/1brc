package fastbrc

import (
	"bytes"
	"math/bits"
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

func indexBytePointerUnsafe8Bytes(bp unsafe.Pointer, length int, needle byte, broadcastedNeedle uint64) int {
	var i int
	for ; i+7 < length; i += 8 {
		xored := *(*uint64)(unsafe.Add(bp, i)) ^ broadcastedNeedle
		mask := (xored - 0x0101010101010101) & ^xored & 0x8080808080808080
		if mask != 0 {
			return bits.TrailingZeros64(mask)>>3 + i
		}
	}
	for ; i < length; i++ {
		if *(*byte)(unsafe.Add(bp, i)) == needle {
			return i
		}
	}
	return -1
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

func ParseFixedPoint16UnsafePtr(bp unsafe.Pointer, length int) int16 {
	i := length - 1
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
	stationTableLen := uint64(len(stationTable))
	stationSize := unsafe.Sizeof(StationInt16{})
	for i := range stationTable {
		stationTable[i].Min = 32767
		stationTable[i].Max = -32767
	}

	var broadcastedDelim uint64 = 0x3b3b3b3b3b3b3b3b
	var broadcastedNl uint64 = 0x0a0a0a0a0a0a0a0a

	for {
		chunk := chunker.NextChunk()
		if chunk == nil {
			break
		}

		startpos := 0
		chunklen := len(*chunk)
		chunkp := unsafe.Pointer(unsafe.SliceData(*chunk))
		for startpos < chunklen {
			// delim := bytes.IndexByte(unsafe.Slice((*byte)(unsafe.Add(chunkp, startpos)), chunklen-startpos-1), ';')
			delim := indexBytePointerUnsafe8Bytes(unsafe.Add(chunkp, startpos), chunklen-startpos, ';', broadcastedDelim)
			//if delim < 0 {
			//	log.Fatal("garbage input, ';' not found")
			//}

			// h := byteHashBCE((*chunkPtr)[startpos:startpos+delim]) % uint32(stationTableLen)
			// h := xxh3.Hash((*chunk)[startpos:startpos+delim]) % stationTableLen

			h := xxh3.Hash(unsafe.Slice((*byte)(unsafe.Add(chunkp, startpos)), delim)) % stationTableLen

			station := (*StationInt16)(unsafe.Add(stationTablePtr, h*uint64(stationSize)))
			if station.N == 0 {
				station.Name = bytes.Clone(unsafe.Slice((*byte)(unsafe.Add(chunkp, startpos)), delim))
				// station.Name = bytes.Clone((*chunk)[startpos : startpos+delim])
			}
			// enable to check if there are collisions :-)
			//if !bytes.Equal(station.Name, (*chunk)[startpos:startpos+delim]) {
			//	panic("woupelai")
			//}

			startpos += delim + 1

			// nl := bytes.IndexByte((*chunk)[startpos:], '\n')
			nl := indexBytePointerUnsafe8Bytes(unsafe.Add(chunkp, startpos), chunklen-startpos, '\n', broadcastedNl)
			//if nl < 0 {
			//	log.Fatal("garbage input, '\\n' not found")
			//}
			//value := (*chunk)[startpos : startpos+nl]

			m := ParseFixedPoint16UnsafePtr(unsafe.Add(chunkp, startpos), nl)
			//if err != nil {
			//	log.Fatal(err)
			//}

			station.NewMeasurement(m)
			startpos += nl + 1
		}

		chunker.ReleaseChunk(chunk)
	}

	return stationTable
}
