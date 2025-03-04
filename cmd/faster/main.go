package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"

	"1brc/internal/brc"
)

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
