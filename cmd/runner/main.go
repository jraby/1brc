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
	nworkers := flag.Int("n", 1, "number of workers for parallel funcs")
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
	case "readsliceint32":
		fmt.Println(brc.ReadSliceInt32(f))
	case "readslicefixed16":
		fmt.Println(brc.ReadSliceFixedInt16(f))
	case "readslicefixed16unsafe":
		fmt.Println(brc.ReadSliceFixedInt16Unsafe(f))
	case "readslicehashfixed16unsafe":
		fmt.Println(brc.ReadSliceStringHashFixedInt16Unsafe(f))
	case "parallelreadslicefixed16unsafe":
		fmt.Println(brc.Carotte(*inputFile))
	case "parallelreadslicefixed16unsafebsearch":
		fmt.Println(brc.ParallelReadSliceFixedInt16UnsafeBSearchNames(*inputFile))
	case "parallelreadslicefixed16unsafeopen":
		fmt.Println(brc.ParallelReadSliceFixedInt16UnsafeOpenAddr(*inputFile))
	case "ParallelReadSlicePatateLineFixedInt16UnsafeOpenAddr":
		fmt.Println(brc.ParallelRunner(*inputFile, *nworkers, brc.ParallelReadSlicePatateLineFixedInt16UnsafeOpenAddr))
	case "ParallelChunkChannelFixedInt16UnsafeOpenAddr":
		fmt.Println(brc.ParallelWorkerRunner(*inputFile, *nworkers, brc.ParallelChunkChannelFixedInt16UnsafeOpenAddr))
	default:
		log.Fatalf("unknown func: %s", *parserFuncName)
	}
}
