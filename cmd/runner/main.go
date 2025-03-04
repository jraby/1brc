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
	parserFuncName := flag.String("funcName", "baseline", "function to call")
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

	switch *parserFuncName {
	case "baseline":
		fmt.Println(brc.Baseline(f))
	case "reduced-allocs":
		fmt.Println(brc.ReducedAllocs(f))
	case "patate":
		fmt.Println(brc.PatateBufferedReader(f))
	case "readslice":
		fmt.Println(brc.ReadSlice(f))
	case "readslicestringhash":
		fmt.Println(brc.ReadSliceStringHash(f))
	default:
		log.Fatalf("unknown func: %s", *parserFuncName)
	}
}
