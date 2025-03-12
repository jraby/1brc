package fastbrc

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChunkerMano(t *testing.T) {
	b := make([]byte, 0, 64*1024+128)
	line := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa patate\n")
	for range 1024*4 + 128 {
		b = append(b, line...)
	}

	reader := bytes.NewReader(b)
	r := bufio.NewReaderSize(reader, 1024*1024)
	b2 := make([]byte, 0, 64*1024+128)
	chunker := NewChunker(r, 1, 255)

	go func() {
		assert.NoError(t, chunker.Run())
	}()
	for {
		chunk := chunker.NextChunk()
		if chunk == nil {
			break
		}
		// log.Printf("size: %d", len(*chunk))
		// log.Printf("chunk: %v", *chunk)
		require.Equalf(t, byte('\n'), (*chunk)[len(*chunk)-1], "chunk: %v", *chunk)
		b2 = append(b2, *chunk...)

		chunker.ReleaseChunk(chunk)
	}
	assert.Equal(t, b, b2)
}

func TestByteChunkerMano(t *testing.T) {
	b := make([]byte, 0, 64*1024+128)
	line := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa patate\n")
	for range 1024*4 + 128 {
		b = append(b, line...)
	}

	b2 := make([]byte, 0, 64*1024+128)
	chunker := NewByteChunker(b, 1, 255)

	go func() {
		assert.NoError(t, chunker.Run())
	}()
	for {
		chunk := chunker.NextChunk()
		if chunk == nil {
			break
		}
		// log.Printf("size: %d", len(*chunk))
		// log.Printf("chunk: %v", *chunk)
		require.Equalf(t, byte('\n'), (*chunk)[len(*chunk)-1], "chunk: %v", *chunk)
		b2 = append(b2, *chunk...)

		chunker.ReleaseChunk(chunk)
	}
	assert.Equal(t, b, b2)
}

func benchmarkMmapByteChunker1b(b *testing.B, nworkers, chCap, chunkSize int) {
	b.ReportAllocs()
	filename := "../../data/1b.txt"
	for range b.N {
		f, err := os.Open(filename)
		if err != nil {
			b.Fatalf("open: %s", err)
		}
		defer f.Close()

		fi, err := f.Stat()
		assert.NoError(b, err)

		size := fi.Size()
		if size < 0 {
			b.Fatalf("mmap: file %q has negative size", filename)
		}
		if size != int64(int(size)) {
			b.Fatalf("mmap: file %q is too large", filename)
		}

		data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
		assert.NoError(b, err)

		chunker := NewByteChunker(data, chCap, chunkSize)
		wg := sync.WaitGroup{}
		wg.Add(nworkers)

		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(b, chunker.Run())
		}()

		for range nworkers {
			go func() {
				defer wg.Done()
				for {
					chunk := chunker.NextChunk()
					if chunk == nil {
						return
					}
					// log.Printf("size: %d", len(*chunk))
					// log.Printf("chunk: %v", *chunk)
					chunker.ReleaseChunk(chunk)
				}
			}()
		}
		wg.Wait()
	}
}

func BenchmarkByteChunker1b(b *testing.B) {
	for nworkers := 1; nworkers <= runtime.NumCPU(); nworkers++ {
		for chCap := nworkers; chCap <= nworkers*4; chCap += 4 {
			for _, chunkSize := range []int{64 << 10, 128 << 10, 256 << 10, 512 << 10, 1024 << 10, 2048 << 10, 4096 << 10} {
				b.Run(fmt.Sprintf("nworker%02d-chcap%03d-chunksize%04dk", nworkers, chCap, chunkSize>>10), func(b *testing.B) {
					benchmarkMmapByteChunker1b(b, nworkers, chCap, chunkSize)
				})
			}
		}
	}
}

func benchmarkChunker1b(b *testing.B, nworkers, chCap, chunkSize int) {
	b.ReportAllocs()
	for range b.N {
		f, err := os.Open("../../data/1b.txt")
		if err != nil {
			b.Fatalf("open: %s", err)
		}
		defer f.Close()
		chunker := NewChunker(f, chCap, chunkSize)
		wg := sync.WaitGroup{}
		wg.Add(nworkers)

		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(b, chunker.Run())
		}()

		for range nworkers {
			go func() {
				defer wg.Done()
				for {
					chunk := chunker.NextChunk()
					if chunk == nil {
						return
					}
					// log.Printf("size: %d", len(*chunk))
					// log.Printf("chunk: %v", *chunk)
					chunker.ReleaseChunk(chunk)
				}
			}()
		}
		wg.Wait()
	}
}

func BenchmarkChunker1b(b *testing.B) {
	for nworkers := 1; nworkers <= runtime.NumCPU(); nworkers++ {
		for chCap := nworkers; chCap <= nworkers*4; chCap += 4 {
			for _, chunkSize := range []int{64 << 10, 128 << 10, 256 << 10, 512 << 10, 1024 << 10, 2048 << 10, 4096 << 10} {
				b.Run(fmt.Sprintf("nworker%02d-chcap%03d-chunksize%04dk", nworkers, chCap, chunkSize>>10), func(b *testing.B) {
					benchmarkChunker1b(b, nworkers, chCap, chunkSize)
				})
			}
		}
	}
}
