package main

import (
	"log"
	"os"
	"testing"
)

func BenchmarkFastBRC(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		f, err := os.Open("data/1b.txt")
		if err != nil {
			log.Fatal(err)
		}
		run(f, 8, 8, 512*1024)

		f.Close()
	}
}
