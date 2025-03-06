package fastbrc

import (
	"bufio"
	"bytes"
	"os"
	"sync"
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

func BenchmarkChunker1b(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		f, err := os.Open("../../data/1b.txt")
		if err != nil {
			b.Fatalf("open: %s", err)
		}
		defer f.Close()
		nworkers := 8
		chunker := NewChunker(f, nworkers*8, 256*1024)
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
