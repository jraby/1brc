package main

import (
	"log"
	"os"
	"testing"

	"1brc/internal/fastbrc"

	"github.com/stretchr/testify/assert"
)

func BenchmarkFastBRCCopyChunker(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		f, err := os.Open("data/1b.txt")
		if err != nil {
			log.Fatal(err)
		}
		chunker := fastbrc.NewChunker(f, 8, 2048*1024)
		run(chunker, 8)

		f.Close()
	}
}

func BenchmarkFastBRCMmapByteChunker(b *testing.B) {
	b.ReportAllocs()
	filename := "data/1b.txt"
	for i := 0; i < b.N; i++ {
		data, err := mmap(filename)
		assert.NoError(b, err)
		chunker := fastbrc.NewByteChunker(data, 24, 2048*1024)
		run(chunker, 24)
	}
}
