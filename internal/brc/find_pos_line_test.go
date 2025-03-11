package brc

import (
	"bytes"
	"log"
	"math/bits"
	"os"
	"sync"
	"testing"
	"unsafe"

	"1brc/internal/fastbrc"

	"github.com/stretchr/testify/assert"
)

func parseChunkReadByteByByteNoop(b []byte) {
	for i := range b {
		_ = b[i]
	}
}

func parseChunkRead4BytesNoop(b []byte) {
	bmax := len(b) - 1

	i := 0
	for ; i <= bmax-3; i += 4 {
		_ = b[i : i+4]
	}

	for ; i <= bmax; i++ {
		_ = b[i]
	}
}

func parseChunkRead8BytesNoop(b []byte) {
	bmax := len(b) - 1

	i := 0
	for ; i <= bmax-7; i += 8 {
		_ = b[i : i+4]
	}

	for ; i <= bmax; i++ {
		_ = b[i]
	}
}

func parseChunkRead16BytesNoop(b []byte) {
	bmax := len(b) - 1

	i := 0
	for ; i <= bmax-15; i += 16 {
		_ = b[i : i+4]
	}

	for ; i <= bmax; i++ {
		_ = b[i]
	}
}

func parseChunkIndexByte(b []byte) {
	startpos := 0
	chunkmaxpos := len(b) - 1
	for startpos <= chunkmaxpos {
		delim := bytes.IndexByte(b[startpos:chunkmaxpos], ';')
		if delim < 0 {
			panic("; not found")
		}
		startpos += delim + 1
		nl := bytes.IndexByte(b[startpos:], '\n')
		if nl < 0 {
			panic("\\n not found")
		}
		startpos += nl + 1
	}
}

func parseChunkIndexByteNoErrCheck(b []byte) {
	startpos := 0
	chunkmaxpos := len(b) - 1
	for startpos <= chunkmaxpos {
		delim := bytes.IndexByte(b[startpos:], ';')
		startpos += delim + 1
		nl := bytes.IndexByte(b[startpos:], '\n')
		startpos += nl + 1
	}
}

func indexByteUnsafe(b []byte, needle byte) int {
	lenb := len(b)
	if lenb == 0 {
		return -1
	}
	i := 0
	bp := unsafe.Pointer(unsafe.SliceData(b))
	for ; i < lenb; i++ {
		if *(*byte)(unsafe.Add(bp, i)) == needle {
			return i
		}
	}
	return -1
}

func indexBytePointerUnsafe4Bytes(bp unsafe.Pointer, length int, needle byte, broadcastedNeedle uint32) int {
	var i int
	for ; i+3 < length; i += 3 {
		xored := *(*uint32)(unsafe.Add(bp, i)) ^ broadcastedNeedle
		mask := (xored - 0x01010101) & ^xored & 0x80808080
		if mask != 0 {
			return bits.TrailingZeros32(mask)>>3 + i
		}
	}
	for ; i < length; i++ {
		if *(*byte)(unsafe.Add(bp, i)) == needle {
			return i
		}
	}
	return -1
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

func parseChunkIndexByteUnsafe(b []byte) {
	startpos := 0
	chunkmaxpos := len(b) - 1
	for startpos <= chunkmaxpos {
		delim := indexByteUnsafe(b[startpos:], ';')
		if delim < 0 {
			panic("; not found")
		}
		startpos += delim + 1
		nl := indexByteUnsafe(b[startpos:], '\n')
		if nl < 0 {
			panic("\\n not found")
		}
		startpos += nl + 1
	}
}

func parseChunkIndexByteUnsafeNoErrCheck(b []byte) {
	startpos := 0
	chunkmaxpos := len(b) - 1
	for startpos <= chunkmaxpos {
		delim := indexByteUnsafe(b[startpos:], ';')
		startpos += delim + 1
		nl := indexByteUnsafe(b[startpos:], '\n')
		startpos += nl + 1
	}
}

func indexByteUnsafe4Bytes(b []byte, needle byte) int {
	broadcastedNeedle := uint32(needle)<<24 | uint32(needle)<<16 | uint32(needle)<<8 | uint32(needle)
	return indexBytePointerUnsafe4Bytes(unsafe.Pointer(unsafe.SliceData(b)), len(b), needle, broadcastedNeedle)
}

func indexByteUnsafe8Bytes(b []byte, needle byte) int {
	broadcastedNeedle := uint64(needle)<<56 | uint64(needle)<<48 | uint64(needle)<<40 | uint64(needle)<<32 | uint64(needle)<<24 | uint64(needle)<<16 | uint64(needle)<<8 | uint64(needle)
	return indexBytePointerUnsafe8Bytes(unsafe.Pointer(unsafe.SliceData(b)), len(b), needle, broadcastedNeedle)
}

func parseChunkPatate(b []byte) {
	startpos := 0

	bp := unsafe.Pointer(unsafe.SliceData(b))
	lenb := len(b)

	broadcastedDelim := 0x3b3b3b3b
	broadcastedNl := 0x0a0a0a0a
	for startpos < lenb {
		delim := indexBytePointerUnsafe4Bytes(unsafe.Add(bp, startpos), lenb-startpos, ';', uint32(broadcastedDelim))
		//if delim < 0 {
		//	panic("; not found")
		//}
		startpos += delim + 1
		nl := indexBytePointerUnsafe4Bytes(unsafe.Add(bp, startpos), lenb-startpos, '\n', uint32(broadcastedNl))
		//if nl < 0 {
		//	panic("\\n not found")
		//}
		startpos += nl + 1
	}
}

func parseChunkPatate8Bytes(b []byte) {
	startpos := 0

	bp := unsafe.Pointer(unsafe.SliceData(b))
	lenb := len(b)

	broadcastedDelim := 0x3b3b3b3b3b3b3b3b
	broadcastedNl := 0x0a0a0a0a0a0a0a0a
	for startpos < lenb {
		delim := indexBytePointerUnsafe8Bytes(unsafe.Add(bp, startpos), lenb-startpos, ';', uint64(broadcastedDelim))
		//if delim < 0 {
		//	panic("; not found")
		//}
		startpos += delim + 1
		nl := indexBytePointerUnsafe8Bytes(unsafe.Add(bp, startpos), lenb-startpos, '\n', uint64(broadcastedNl))
		//if nl < 0 {
		//	panic("\\n not found")
		//}
		startpos += nl + 1
	}
}

func parseChunkIndexByteUnsafe4Bytes(b []byte) {
	startpos := 0
	chunkmaxpos := len(b) - 1
	for startpos <= chunkmaxpos {
		delim := indexByteUnsafe4Bytes(b[startpos:], ';')
		if delim < 0 {
			panic("; not found")
		}
		startpos += delim + 1
		nl := indexByteUnsafe4Bytes(b[startpos:], '\n')
		if nl < 0 {
			panic("\\n not found")
		}
		startpos += nl + 1
	}
}

func parseChunkIndexByteUnsafe4BytesNoErrCheck(b []byte) {
	startpos := 0
	chunkmaxpos := len(b) - 1
	for startpos <= chunkmaxpos {
		delim := indexByteUnsafe(b[startpos:], ';')
		startpos += delim + 1
		nl := indexByteUnsafe(b[startpos:], '\n')
		startpos += nl + 1
	}
}

func TestIndexByteImpl(t *testing.T) {
	implementations := map[string]struct {
		ibImpl func([]byte, byte) int
	}{
		"indexByteUnsafe":       {indexByteUnsafe},
		"indexByteUnsafe4Bytes": {indexByteUnsafe4Bytes},
		"indexByteUnsafe8Bytes": {indexByteUnsafe8Bytes},
	}

	tcs := []struct {
		b      []byte
		needle byte
	}{
		{nil, 'a'},
		{[]byte{}, 'a'},
		{[]byte("bbbbbbbbbbbbbbbbbba"), 'a'},
		{[]byte("bbbbbbbbbbbbbbbbbba"), 'b'},
		{[]byte("bbbbbbbbbbbbbbbbbba"), 'c'},
		{[]byte("bbddddddddddddddddddddddddddddddddddddddddddbbbbbbbbbbbbbbbba"), 'c'},
		{[]byte("bbdddddddddddddddddddddddddddddWddddddddddddbbbbbbbbbbbbbbbba"), 'W'},
	}

	for name, imp := range implementations {
		for _, tc := range tcs {
			t.Run(name+"-"+string(tc.b)+"-"+string(tc.needle), func(t *testing.T) {
				assert.Equal(t, bytes.IndexByte(tc.b, tc.needle), imp.ibImpl(tc.b, tc.needle))
			})
		}
	}
}

func BenchmarkChunkLineScan1b(b *testing.B) {
	benchmarkChunkLineScan(b, "../../data/1b.txt")
}

func benchmarkChunkLineScan(b *testing.B, inputFile string) {
	tcs := map[string]struct {
		parserFunc func([]byte)
	}{
		"Noop":                            {func([]byte) {}},
		"ReadByteByByteNoop":              {parseChunkReadByteByByteNoop},
		"Read4BytesNoop":                  {parseChunkRead4BytesNoop},
		"Read8BytesNoop":                  {parseChunkRead8BytesNoop},
		"Read16BytesNoop":                 {parseChunkRead16BytesNoop},
		"IndexByte":                       {parseChunkIndexByte},
		"IndexByteNoErrCheck":             {parseChunkIndexByteNoErrCheck},
		"IndexByteUnsafe":                 {parseChunkIndexByteUnsafe},
		"IndexByteUnsafeNoErrCheck":       {parseChunkIndexByteUnsafeNoErrCheck},
		"IndexByteUnsafe4Bytes":           {parseChunkIndexByteUnsafe4Bytes},
		"IndexByteUnsafe4BytesNoErrCheck": {parseChunkIndexByteUnsafe4BytesNoErrCheck},
		"IndexBytePatate":                 {parseChunkPatate},
		"IndexBytePatate8Bytes":           {parseChunkPatate8Bytes},
	}

	for name, tc := range tcs {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {

				f, err := os.Open(inputFile)
				if err != nil {
					log.Fatal(err)
				}
				defer f.Close()

				chunker := fastbrc.NewChunker(f, 1, 512*1024)
				go chunker.Run()

				wg := sync.WaitGroup{}
				nworkers := 8
				wg.Add(nworkers)
				for range nworkers {
					go func() {
						defer wg.Done()
						for {
							chunk := chunker.NextChunk()
							if chunk == nil {
								break
							}
							// log.Printf("chunk: %d", len(*chunk))
							tc.parserFunc(*chunk)
							chunker.ReleaseChunk(chunk)
						}
					}()
				}
				wg.Wait()

			}
		})
	}
}
