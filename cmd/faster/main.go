package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"

	"1brc/internal/brc"
)

type Station struct {
	Min   float64
	Max   float64
	Total float64
	N     int64
}

func (s *Station) NewMeasurement(m float64) {
	if m < s.Min {
		s.Min = m
	}
	if m > s.Max {
		s.Max = m
	}
	s.Total += m
	s.N++
}

func NewStation(m float64) *Station {
	return &Station{
		Min:   m,
		Max:   m,
		Total: m,
		N:     1,
	}
}

type StationScanner struct {
	buf    []byte
	reader io.Reader
}

func main() {
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	inputFile := flag.String("i", "data/10m.txt", "input file")
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	f, err := os.Open(*inputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	fmt.Println(brc.ReducedAllocs(f))
}
