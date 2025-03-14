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

func BenchmarkReadSliceStringHash10m(b *testing.B) {
	benchmark(b, ReadSliceStringHash, "../../data/10m.txt")
}
func BenchmarkReadSlice10m(b *testing.B)      { benchmark(b, ReadSlice, "../../data/10m.txt") }
func BenchmarkReadSliceInt3210m(b *testing.B) { benchmark(b, ReadSliceInt32, "../../data/10m.txt") }
func BenchmarkReadSliceFixed1610m(b *testing.B) {
	benchmark(b, ReadSliceFixedInt16, "../../data/10m.txt")
}

func BenchmarkReadSliceFixed16Unsafe10m(b *testing.B) {
	benchmark(b, ReadSliceFixedInt16Unsafe, "../../data/10m.txt")
}

func BenchmarkParallelReadSliceFixedInt16Unsafe10m(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Carotte("../../data/10m.txt")
	}
}

func BenchmarkParallelReadSliceFixedInt16UnsafeBsearch10m(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ParallelReadSliceFixedInt16UnsafeBSearchNames("../../data/10m.txt")
	}
}

func BenchmarkParallelReadSliceFixedInt16UnsafeOpen10m(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ParallelReadSliceFixedInt16UnsafeOpenAddr("../../data/10m.txt")
	}
}

func BenchmarkParallelReadSlicePatateLineFixedInt16UnsafeOpenAddr10m(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ParallelRunner("../../data/10m.txt", 8, ParallelReadSlicePatateLineFixedInt16UnsafeOpenAddr)
	}
}

func BenchmarkParallelChunkChannelFixedInt16UnsafeOpenAddr10m(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ParallelWorkerRunner("../../data/10m.txt", 8, ParallelChunkChannelFixedInt16UnsafeOpenAddr)
	}
}

func BenchmarkParallelChunkChannelFixedInt16UnsafeOpenAddr1b(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ParallelWorkerRunner("../../data/1b.txt", 8, ParallelChunkChannelFixedInt16UnsafeOpenAddr)
	}
}

func BenchmarkParallelReadSliceFixedInt16Unsafe1b(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Carotte("../../data/1b.txt")
	}
}

func BenchmarkReadSliceStringHashFixed16Unsafe10m(b *testing.B) {
	benchmark(b, ReadSliceStringHashFixedInt16Unsafe, "../../data/10m.txt")
}

func BenchmarkReadSliceMmap10m(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ReadSliceMmap("../../data/10m.txt")
	}
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
