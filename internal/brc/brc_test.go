package brc

import (
	"io"
	"log"
	"os"
	"testing"
)

func benchmark(b *testing.B, parserFunc func(io.Reader) string, inputFile string) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {

		f, err := os.Open(inputFile)
		if err != nil {
			log.Fatal(err)
		}
		parserFunc(f)

		f.Close()
	}
}

func BenchmarkBaseline10m(b *testing.B)      { benchmark(b, Baseline, "../../data/10m.txt") }
func BenchmarkReducedAllocs10m(b *testing.B) { benchmark(b, ReducedAllocs, "../../data/10m.txt") }
func BenchmarkReducedAllocsBufferedReader10m(b *testing.B) {
	benchmark(b, ReducedAllocsBufferedReader, "../../data/10m.txt")
}

func BenchmarkReducedAllocsMmap10m(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ReducedAllocsMmapReader("../../data/10m.txt")
	}
}

func BenchmarkHandParserMmap10m(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		HandParserMmap("../../data/10m.txt")
	}
}

func BenchmarkPatateParserMmap10m(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// b.Log(PatateMmapReader("../../data/10m.txt"))
		PatateMmapReader("../../data/10m.txt")
	}
}

func BenchmarkPatateBufferedReader10m(b *testing.B) {
	benchmark(b, PatateBufferedReader, "../../data/10m.txt")
}
