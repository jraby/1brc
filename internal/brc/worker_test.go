package brc

import (
	"bytes"
	"crypto/md5"
	"io"
	"log"
	"os"
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

	hasher := md5.New()
	receivedBytes := 0
	ch := chunker(f, 8)
	for chunk := range ch {
		// log.Printf("size: %d", len(*chunk))
		// log.Printf("chunk: %v", *chunk)
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
	// hasher.Write([]byte("\n"))

	assert.Equal(t, hasher.Sum(nil), chunkermd5)
}

func TestChunkerMano(t *testing.T) {
	b := make([]byte, 0, 64*1024+128)
	line := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa patate\n")
	for range 1024*4 + 128 {
		b = append(b, line...)
	}

	b2 := make([]byte, 0, 64*1024+128)
	ch := chunker(bytes.NewReader(b), 8)
	for chunk := range ch {
		log.Printf("size: %d", len(*chunk))
		// log.Printf("chunk: %v", *chunk)
		require.Equalf(t, byte('\n'), (*chunk)[len(*chunk)-1], "chunk: %v", *chunk)
		b2 = append(b2, *chunk...)

		ChunkPool.Put(chunk)
	}
	assert.Equal(t, b, b2)
}
