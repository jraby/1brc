package fastbrc

import (
	"bytes"
	"math/bits"
	"unsafe"
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

// like indexbyte, but works on 8 bytes at a time
// needle and broadcastedNeedle are needed to avoid calculating it everytime,
// and busting the inlining budget
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

// Same as ParseFixedPoint16Unsafe, but works with unsafe.Pointer and length.
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
		var delim, nl int
		for startpos < chunklen {

			// XXX this will access memory past the chunk if the data is invalid.
			delim = indexBytePointerUnsafe8Bytes(unsafe.Add(chunkp, startpos), 32, ';', broadcastedDelim)
			//if delim < 0 {
			//	log.Fatal("garbage input, ';' not found")
			//}

			// h := xxh3.Hash(unsafe.Slice((*byte)(unsafe.Add(chunkp, startpos)), delim))

			// inlined hashAny from xxh3. original source: https://github.com/zeebo/xxh3
			// see xxh3.go for full license
			// Trimmed down to support input <32 char since the longest station is 26 byte long
			var acc u64
			p := unsafe.Add(chunkp, startpos)
			l := delim
			var h u64

			switch {
			case l <= 16:
				switch {
				case l > 8: // 9-16
					inputlo := readU64(p, 0) ^ (key64_024 ^ key64_032)
					inputhi := readU64(p, ui(l)-8) ^ (key64_040 ^ key64_048)
					folded := mulFold64(inputlo, inputhi)
					h = xxh3Avalanche(u64(l) + bits.ReverseBytes64(inputlo) + inputhi + folded)

				case l > 3: // 4-8
					input1 := readU32(p, 0)
					input2 := readU32(p, ui(l)-4)
					input64 := u64(input2) + u64(input1)<<32
					keyed := input64 ^ (key64_008 ^ key64_016)
					h = rrmxmx(keyed, u64(l))

				case l == 3: // 3
					c12 := u64(readU16(p, 0))
					c3 := u64(readU8(p, 2))
					acc = c12<<16 + c3 + 3<<8
					acc ^= u64(key32_000 ^ key32_004)
					h = xxhAvalancheSmall(acc)

				case l > 1: // 2
					c12 := u64(readU16(p, 0))
					acc = c12*(1<<24+1)>>8 + 2<<8
					acc ^= u64(key32_000 ^ key32_004)
					h = xxhAvalancheSmall(acc)

				case l == 1: // 1
					c1 := u64(readU8(p, 0))
					acc = c1*(1<<24+1<<16+1) + 1<<8
					acc ^= u64(key32_000 ^ key32_004)
					h = xxhAvalancheSmall(acc)

				default: // 0
					h = 0x2d06800538d394c2 // xxh_avalanche(key64_056 ^ key64_064)
				}

			case l < 32:
				acc = u64(l) * prime64_1

				acc += mulFold64(readU64(p, 0*8)^key64_000, readU64(p, 1*8)^key64_008)
				acc += mulFold64(readU64(p, ui(l)-2*8)^key64_016, readU64(p, ui(l)-1*8)^key64_024)
				h = xxh3Avalanche(acc)
			default:
				panic("input to baby xxh3 too long")
			}

			station := (*StationInt16)(unsafe.Add(stationTablePtr, (h%stationTableLen)*uint64(stationSize)))
			if station.N == 0 {
				station.Name = bytes.Clone(unsafe.Slice((*byte)(unsafe.Add(chunkp, startpos)), delim))
			}
			// enable to check if there are collisions :-)
			//if !bytes.Equal(station.Name, (*chunk)[startpos:startpos+delim]) {
			//	log.Printf("h: %d", h)
			//	log.Printf("station: %s", string(station.Name))
			//	log.Printf("name: %s", string((*chunk)[startpos:startpos+delim]))
			//	panic("woupelai")
			//}

			startpos += delim + 1

			// XXX this will access memory past the chunk if the data is invalid.
			nl = indexBytePointerUnsafe8Bytes(unsafe.Add(chunkp, startpos), 8, '\n', broadcastedNl)
			//if nl < 0 {
			//	log.Fatal("garbage input, '\\n' not found")
			//}

			m := ParseFixedPoint16UnsafePtr(unsafe.Add(chunkp, startpos), nl)

			station.NewMeasurement(m)
			startpos += nl + 1
		}

		chunker.ReleaseChunk(chunk)
	}

	return stationTable
}
