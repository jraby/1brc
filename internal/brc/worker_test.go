package brc

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"io"
	"log"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChunker(t *testing.T) {
	f, err := os.Open("../../data/1m.txt")
	if err != nil {
		t.Fatalf("open: %s", err)
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 256*1024)

	hasher := md5.New()
	receivedBytes := 0
	ch := chunker(reader, 8)
	for chunk := range ch {
		require.Equalf(t, byte('\n'), (*chunk)[len(*chunk)-1], "chunk: %v", *chunk)

		_, err = hasher.Write(*chunk)
		assert.NoError(t, err)
		receivedBytes += len(*chunk)

		ChunkPool.Put(chunk)
	}

	chunkermd5 := hasher.Sum(nil)

	stat, err := f.Stat()
	assert.NoError(t, err)

	assert.Equal(t, int(stat.Size()), receivedBytes)
	f, err = os.Open("../../data/1m.txt")
	if err != nil {
		t.Fatalf("open: %s", err)
	}
	hasher.Reset()
	if _, err := io.Copy(hasher, f); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, hasher.Sum(nil), chunkermd5)
}

func TestChunkerMano(t *testing.T) {
	b := make([]byte, 0, 64*1024+128)
	line := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa patate\n")
	for range 1024*4 + 128 {
		b = append(b, line...)
	}

	reader := bytes.NewReader(b)
	r := bufio.NewReaderSize(reader, 1024*1024)
	b2 := make([]byte, 0, 64*1024+128)
	ch := chunker(r, 8)
	for chunk := range ch {
		log.Printf("size: %d", len(*chunk))
		// log.Printf("chunk: %v", *chunk)
		require.Equalf(t, byte('\n'), (*chunk)[len(*chunk)-1], "chunk: %v", *chunk)
		b2 = append(b2, *chunk...)

		ChunkPool.Put(chunk)
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
		ch := chunker(f, 8)
		wg := sync.WaitGroup{}
		nworkers := 8
		wg.Add(nworkers)

		for range nworkers {
			go func() {
				defer wg.Done()
				for chunk := range ch {
					// log.Printf("size: %d", len(*chunk))
					// log.Printf("chunk: %v", *chunk)
					ChunkPool.Put(chunk)
				}
			}()
		}
		wg.Wait()
	}
}
