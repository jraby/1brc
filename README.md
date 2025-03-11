The following is a summary of a few hacking sessions trying to write a program with decent performance to complete the [1 billion rows challenge](https://github.com/gunnarmorling/1brc).

The final code can be found in the [fastbrc](./internal/fastbrc/) module.
To run it:
1. generate the measurement file as per https://github.com/gunnarmorling/1brc#running-the-challenge) and store it as `data/1b.txt`
1. run `make run`

---

The idea is to write a program that tracks the minimum, maximum and average value for each unique "station" in the input file and write the result to `stdout`.

The input file has 1 billion rows and is about 13gb in size.
The format is:
```
<stationname>;<measurement>\n
```

Where `<stationName>` is the name of a city encoded as a UTF-8 string
and `<measurement>` is a string representing a float value with a single fractional digit (one decimal).

For example:
```
Montreal;-99.9
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
St. John's;15.2
Cracow;12.6
Bridgetown;26.9
Istanbul;6.2
Roseau;34.4
Conakry;31.2
Istanbul;23.0
```

The minimum measurement is `-99.9` and the maximum is `99.9`.

The program must do its works at runtime, it is not permitted to bake results or tables into the program.

The output format is:
```
{ <station>=<min>/<avg>/<max>, ... }
```
The stations must be listed in alphabetical order.


---


I have a bit of free time right now, so I thought it would be fun to spend *some* time experimenting and brushing up on skills I hadn't used in a while (or ever really).

I really enjoyed the process of optimization:
- Run a benchmark on a subset of the dataset (10m rows) and record a cpu and memory profile.
- observe the profiles with pprof
- choose an area to explore
- hack on it, profile, repeat

I ended up with a dozen implementations and quite a few failed experiments and red herrings.
Overall it was quite interesting.

In the end, I stopped when the program could run in 1.3s on a ryzen 9 7900 (24 cores) machine.
On that machine, the #1 entry in the leaderboard runs in 0.488ms.

# Single thread baseline

A  straightforward single thread implementation using:
- `map[string]*Station` to accumulate the measurements
- `bufio.ScanLines` to get each line
- `strings.Split` to split each line on `;`
- `strconv.ParseFloat` to convert the measurement to float

Runs through the 13gb file in:
- ~94s on a i7-7700
- ~88s on a ryzen 9 7900

The code can be found [here](https://github.com/jraby/1brc/blob/main/internal/brc/baseline.go#L140-L177), run with  `make runner.baseline`.

This approach is not very efficient:
- bufio.Scanner uses a small buffer (4k) to read the input file, leading to a big syscall overhead
- `strings.Split` and `scanner.Text()` allocate in the hot loop.

Overall the time is spent:
- 30% in `string.Split` (half of this in `runtime.makeslice`)
- 18% in `strconv.ParseFloat`
- 17% in `scanner.Text()` (all of it in allocation routines)
- 14% in map access
- 10% in `scanner.Scan()`
- 05% in the garbage collector
- 04% in the function that accumulates measurements

Clearly there was room for improvement.

# Targets

This program does a few things in a hot loop:
- read the input file
- split each input line in 2 parts: `name` and `measurement`
- convert the measurement from string  to a number representation
- find the accumulated stats for `name`
- accumulate the current stat

One clear goal is to avoid allocation and copies in most of these steps.

At first I was focussing on single thread performance since I expected to be able to parallelize this workload without too much effort by having a goroutine find valid chunks of data in the input file and send them to some worker goroutine that would independently do the parsing and accumulate the stats. Then merge the results.

## Input reading and splitting 1

I tried various approaches to reduce the number of allocation and string copies.

first attempt:
- mmap the input file
- work with `scanner.Bytes` to avoid allocation
- use `bytes.IndexByte` to locate the `;`
- use `unsafe.String` and `unsafe.SliceData` to create a temporaty string for the measurement without allocating, and pass that to `strconv.ParseFloat`

This takes the total time down to: 
- 40s on i7-7700
- 38s on ryzen 9 7900

The code can be found [here](https://github.com/jraby/1brc/blob/main/internal/brc/reduced_allocs.go#L28-L72), run with `make runner.reduced-allocs`.

The time is now split like this:
- 31% `strconv.ParseFloat`
- 30% map access
- 25% `scanner.Scan` with more than a third of it being spent reading the input file
- 06% `bytes.IndexByte(b, ';')`
- 01% new measurements

Using `bufio.NewReaderSize(reader, 1024*1024)` to read the input file in chunks of 1mb instead of the default 4k for the scanner reduces the runtime to: (`make runner.reduced-allocs-buffered`)
- 39s on i7-7700
- 37s on ryzen 9 7900

After that I made a failed attempts at trying to read every byte only once, by using `IndexByte(b, delim)`, where `delim` was either `;` or `\n` depending on the state of the parser.
It was terrible, adding a `if` statement in the middle of that loop along with a variable, destroyed the performance, it was something like twice as slow.

Eventually I tried using `bufio.(*Reader).ReadSlice('\n')` to find end of lines, instead of `bufio.ScanLines`:
- 36.5s on i7-7700  (for a still unknown reason, this measurement went up to 57s during writing of this document, so I'll only be using the ryzen for reference)
- 35.5s on ryzen 9 7900

The code can be found [here](https://github.com/jraby/1brc/blob/main/internal/brc/readslice.go#L23-L76), run with `make runner.readslice`.

At this point, the processing is now split like this:
- 38% `strconv.ParseFloat`
- 33% map access
- 15% `bufio.(*Reader).ReadSlice('\n')`
- 06% `IndexByte(b, ';')`
- 04%  new measurement

I switched to map access profiling at that point, but would return to reading and parsing lines later.

## Map Access 1

Keeping the same input parsing as above, I then proceeded to try different data structures for storing the `name` to `measurement` mapping.

I wrote a rudimentary hash table with 8192 buckets, hashing the `name` strings using various algorithms.
I would come back to these hashing algorithm later, but at this point I tested, `fnv1a` 32 bits from the stdlib, [xxhash](github.com/cespare/xxhash) and an unrolled version of fnv1a32 that processes 4 bytes of input per iteration instead of doing it 1 byte at a time.

See `stringHash` in 1brc/internal/brc/hash.go.

It ended up taking ~34s on ryzen 9 7900.

The code can be found [here](https://github.com/jraby/1brc/blob/main/internal/brc/readslice.go#L23-L76), run with `make runner.readslicestringhash`.

The map access went down from 33% of runtime to 27%,
but it introduced a bunch of allocations and I wasn't satisfied with it.

I let it on the back burner for the moment and moved to a first iteration on float parsing for a change of scenery.

## Float parsing 1

Since the challenge only required 1 decimal place precision in the output, I tried to parse the input as a int, multiplying the value by 10.
So 12.3 would be stored as 123.
This would eliminate all floating point math during the hot loop and require divisions by 10 when printing the output.

I wrote a somewhat robust parser ([`ParseFixedPoint16`](./internal/brc/parse_fixed_point.go)) and switched to using a `StationInt16` stuct that uses int16 for Min,Max and int32 for the Total and number of samples.
This parser works on a byte slice (so we can drop the `unsafe.String` incantation from the parse loop).
It scans the slice forward, checking for a leading `-` for negative numbers,
it stop parsing after the first decimal is read,
it checks for over / under flow, it aborts if there's more than one `.` in the input and aborts if there's an invalid character in the input.

This runs the challenge in 27s on the ryzen 9 7900.

The code can be found [here](https://github.com/jraby/1brc/blob/main/internal/brc/readslice.go#L183-L245), run with `make runner.readslicefixed16`.

Float parsing went from 38% to 17%.
We're getting somewhere.

Profiling now shows:
- 45% map access
- 18% `ReadSlice('\n')`
- 17% `ParseFixedPoint16`
- 08% `IndexByte(b, ';')`
- 03% new measurement

Since the input is known to be valid,
I removed all checks for invalid stuff, keeping only the logic for `.` skipping and sign flip.
I also started scanning from the end of the slice.
See [ParseFixedPoint16Unsafe](./internal/brc/parse_fixed_point.go).

This runs in 24s on the ryzen.

The code can be found [here](https://github.com/jraby/1brc/blob/main/internal/brc/readslice.go#L247-L302), run with `make runner.readslicefixed16unsafe` (*weird name, it isn't using unsafe, but it is not safe for all input*)

Profiling shows:
- 50% map access
- 21% `ReadSlice('\n')`
- 10% `ParseFixedPoint16Unsafe`
- 08% `IndexByte(b, ';')
- 06% new measurement

Clearly the next target should be reducing the time spent to get accumulated stats for known stations.
But I didn't feel like going down that rabbit hole yet, so I worked on parallelization since I expected it to be simple.

## Parallelization 1

To parallelize the workload, I started by:
- mmap'ing the input file
- splitting the files in N valid sections of about totalSize/N size, taking care to end each section on a `\n`.
- creating a [`io.SectionReader`](https://pkg.go.dev/io#SectionReader) for each section.

Then spin up N goroutines that would parse through each section in concurrently.
Once the goroutines are done, merge the results and print the output.

This takes 2.23s on the ryzen 9 7900 with 24 cores.

The code can be found [here](https://github.com/jraby/1brc/blob/main/internal/brc/parallel.go#L16-L119), run with `make runner.parallelreadslicefixed16unsafe`.

Profiling shows:
- 47% map access
- 18% `ReadSlice('\n')`
- 12% `IndexByte(b, ';')`
- 10% `ParseFixedPoint16Unsafe`
- 06% new measurement

It doesn't scale linearly, but 24 cores is still the fastest:
```
nproc=1       25.258936986 seconds time elapsed
nproc=2       12.736176446 seconds time elapsed
nproc=3        8.795306467 seconds time elapsed
nproc=4        6.933476025 seconds time elapsed
nproc=5        5.481164340 seconds time elapsed
nproc=6        4.730700377 seconds time elapsed
nproc=7        4.122684611 seconds time elapsed
nproc=8        3.722031584 seconds time elapsed
nproc=9        3.376391007 seconds time elapsed
nproc=10       3.111426008 seconds time elapsed
nproc=11       2.892948532 seconds time elapsed
nproc=12       2.786187048 seconds time elapsed
nproc=13       2.830006601 seconds time elapsed
nproc=14       2.647819639 seconds time elapsed
nproc=15       2.655925959 seconds time elapsed
nproc=16       2.643987080 seconds time elapsed
nproc=17       2.595265458 seconds time elapsed
nproc=18       2.538244186 seconds time elapsed
nproc=19       2.462673918 seconds time elapsed
nproc=20       2.430625413 seconds time elapsed
nproc=21       2.387148360 seconds time elapsed
nproc=22       2.323841129 seconds time elapsed
nproc=23       2.295491394 seconds time elapsed
nproc=24       2.263671349 seconds time elapsed
```

## Map Access 2

Clearly it was time to reduce map access time.

I experimented with bunch of approaches:
- binary search in an array, that was terrible, comparing keys multiple times with `bytes.Compare` is a good way to waste time :-)
- radix trees. The implementation allocated a lot, I didn't pursue it further.
- switching `StringHash` to `ByteHash`, that is, avoid a conversion from byte to string (via `unsafe.String`) and do the unrolled fnv1a32 on a byte slice.
- using a big array to store `StationInt16` struct without pointers and no collision management. (cowboy hat)
- multiple tries to get the  `StringHash` hash table to go reasonably fast.
  I managed to remove most allocations, try various hash (murmur3, crc, unrolled fnv1a32), 
  it was faster than the original for sure, but slow than the "big array" approach.

The "big array" idea is a bit dumb but it is fast: there's ~300 unique names in the input file, what if the hash function didn't have any collision for the input?
It turns out that fnv1a32 doesn't have collisions for the input set when addressing with `fnv(name) % len(array)` when the array length is 65535.
(this might be considered cheating :-)

One thing to note, adding collision detection via `bytes.Equal(station.Name, name)` adds ~200ms to the overall processing on the ryzen.
So this code uncommented only when touching the hash function, and validating the output.
When running benchmarks, I would simply remove the check.

This approach takes 1.8s on the ryzen (24 cores) (`runner.parallelreadslicefixed16unsafeopen` -- another weird name...) 

The code can be found [here](https://github.com/jraby/1brc/blob/main/internal/brc/parallel.go#L255-L324).

Profling shows:
- 25% `byteHash`
- 25% `ReadSlice('\n')`
- 15% `IndexByte(';')`
- 12% `ParseFixedPoint16Unsafe`
- 03% new measurement

So while it is not "clean" and certainly not safe, it works for the input set and it is quite fast.

## Input reading and splitting 2

At this point I went by to try and get the data faster.

### ReadSlice(';') + ReadSlice('\n')
I tried using `ReadSlice(';')` followed by `ReadSlice('\n')` while keeping the rest as above and to my surprise, it was slower.
It turns out that `ReadSlice` does a fair bit of memory copying to "slide" the data within its internal buffer when refilling it.

This approach took 2.25s on ryzen.

The code can be found [here](https://github.com/jraby/1brc/blob/main/internal/brc/parallel.go#L445-L488).


### Chunker

I moved to another approach:
- have a single goroutine read the input file (the chunker)
- get a `*[]byte` buffer from a sync.Pool (called a `chunk`)
- fill the buffer with data from the file, backtracking to the last `\n` in the buffer (and keeping the leftover bytes around)
- push the chunk to a channel

Each worker now reads its input from the chunker's channel:
- while there's a chunk in the channel
  - find next `;` in the buffer with `IndexByte(';')`
  - lookup the `StationInt16` with the name between `startpos` and `;` (with the `fnv(name) % len(array)` approach described above)
  - find next `\n` in buffer with `IndexByte('\n')`
  - parse the float
  - adjust `startpos`
  - loop until end of chunk

This approach takes 1.62s on the ryzen.

The code:
- [chunker](https://github.com/jraby/1brc/blob/main/internal/fastbrc/chunker.go)
- [worker](https://github.com/jraby/1brc/blob/main/internal/fastbrc/parse_worker.go)

Run with `make runner.ParallelChunkChannelFixedInt16UnsafeOpenAddr`

Profiling:
- 29% `byteHash`
- 28% `IndexByte` (both `;` and `\n`)
- 11% `ParseFixedPoint16Unsafe`
- 06% new measurement
- 03% file read in chunker
The rest is not shown in profiling, it is spent in `ParallelChunkChannelFixedInt16UnsafeOpenAddr`

# refactor
I thought I was mostly done, so I took a little break here and shuffled to "best" code around a little bit sincee it was starting to be a mess of tests and benchmark.

The fast code is now in `internal/fastbrc` and there's a `cmd/fastbrc` command that calls it.

Unfortunately, I wasn't done, I started iterating on a single copy of the code instead of keeping all versions,
meaning I can't quickly rerun the benchmarks to see what impact each change had on the runtime.
So the following data is taken from my notes and comments in the code.

# Bound checking and unsafe slice access shenanigans

At this point I was starting to run out of ideas that didn't require drastically changing how I approached the problem.

I knew there was probably a way to parse the float in a branchless way with some bit twiddling magic, but I wasn't up for that.
Instead I explored the generated assembly of some of the very hot functions, like the fnv hash.

The assembly can be seen with `go tool objdump -S -s funcname binaryname`.
(and if the function is inlined, it won't be searchable with `-s`, so one needs to search for the caller function)

What I saw was a bit surprising, there were 5 bound checks in the function, one for each access in the unrolled loop, and another one in the "leftovers" loop.

I tried to add some compiler hints to let it know that *this is fine™*, but to no avail.

That's when I started to get dirty :)
The result is [`ByteHashBCE`](https://github.com/jraby/1brc/blob/main/internal/fastbrc/parse_worker.go#L37-L67), where `unsafe.Pointer` and `unsafe.Add` are used to access the underlying data of the byte slice.

According to my notes, this made the fnv hash go 10% faster both on i7-7700 and the ryzen.

So went and replaced byte slice access with `unsafe.Pointer` + `unsafe.Add` where it seemed very hot.
Notably in `ParseFixedPoint16Unsafe`, which was also rewritten to avoid any conditionals in the loop.
Instead of looping from the back of the value, it now:
- reads the last byte and converts it
- skips the dot
- loop and convert until to position 0
- check if position 0 is a `-` sign, and flip the sign, or convert the first digit.

Access to the big `StationInt16` array has also been updated to use pointer math to avoid the bound check.

these 3 changes took the time from 1.62s on ryzen down to 1.39s

Run with `make run`.

# xxh3

While trying to come up with a way to do the fnv1a hash 4 bytes at a time instead of byte by byte,
(which would work, but would give the same hash value), I stumbled upon (ahem, chatgpt suggested...) [`xxh3`](https://github.com/Cyan4973/xxHash).

That thing is fast for this dataset:
```
cpu: AMD Ryzen 9 7900 12-Core Processor
BenchmarkHashByteXxh3-24                 1323704               906.4 ns/op
BenchmarkHashByteXxHashNew64-24           288284              4130 ns/op
BenchmarkHashByteXxHash64-24             1000000              1120 ns/op
BenchmarkHashFnv1a-24                    1000000              1013 ns/op
BenchmarkHashFnv1aRangeIndex-24           750393              1601 ns/op
BenchmarkHashFnv1aRange-24               1000000              1005 ns/op
BenchmarkHashFnv1aUnrolled4-24            941508              1249 ns/op
BenchmarkHashFnv1aUnrolledBCE-24         1000000              1164 ns/op
BenchmarkHashStdlibFnv1a-24               814526              1384 ns/op

cpu: Intel(R) Core(TM) i7-7700 CPU @ 3.60GHz
BenchmarkHashByteXxh3-8                   735426              1608 ns/op
BenchmarkHashByteXxHashNew64-8            180835              6438 ns/op
BenchmarkHashByteXxHash64-8               547801              2202 ns/op
BenchmarkHashFnv1a-8                      438271              2512 ns/op
BenchmarkHashFnv1aRangeIndex-8            390988              3010 ns/op
BenchmarkHashFnv1aRange-8                 352113              3222 ns/op
BenchmarkHashFnv1aUnrolled4-8             492810              2194 ns/op
BenchmarkHashFnv1aUnrolledBCE-8           620570              1921 ns/op
BenchmarkHashStdlibFnv1a-8                458587              2531 ns/op
```
this is the time taken to hash every city from the dataset 8 times.

xxh3 is faster than the fastest fnv by ~10%.

One strange thing here, the following implementation of fnv1a is much faster than the 4 bytes unrolled version on the ryzen, but slower on the i7-7700:
```
	const prime32 = uint32(16777619)
	hash := uint32(2166136261)

	length := len(b)
	for i := 0; i < length; i++ {
		hash ^= uint32(b[i])
		hash *= prime32
	}

	return hash
``` 
I'm not sure what is happening with that.

In any case, xxh3 is still faster, so I was happy to drop that code.

Changing from fnv to xxh3 brought the time down from 1.39s to 1.30s on the ryzen.

# faster bytes.Equal

While experimenting with accessing multiple bytes at a time for comparison, I ended up writing `fastbyteequal` (see [internal/brc/station_find_test.go]) which compares 2 byte slices for equality 4 bytes at a time and doing the remainder one by one.
In my rudimentary test, it seems to be around 5% faster than bytes.Equal on both the i7-7700 and ryzen 9 7900, which I found quite surprising.

I didn't end up using that code, since there was no collision in the bigarray with xxh3, but that would have been a way to lose less performance.


# Conclusion

In the end, the program takes 1.30s on a ryzen 9 7900 (24 core), while the baseline implementation without concurrency took 88s on the same machine.
The profiling show:
- 30% `IndexByte`
- 29% in ParseWorker
- 23% `xxh3`
- 10% `ParseFixedPoint16Unsafe`
- 05% new measurement
- 03% file reading

The number one entry on the leaderboard runs in 0.448ms on that machine, so clearly there's room for improvement,
but I think that's enough for now.

It was very interesting to explore the multiple sides of this problem for a few hacking session.
It is quite simple on the surface, but there's a lot of depth to it!

I think there are some tweaks to squeeze more performance out of this:
- remove all bound checking from the ParseWorker loop
- conversion from string to fixed precision int without any branches, using bit twiddling (like they did in the #1 entry)
- change chunker to use slices from mmap instead of copying the data
  - this is useless at the moment since the rest the of program is faster than the file reading part (1.3s vs ~0.5s), but it is probably the only way to go under 500ms like the #1 entry does.

In the end, I guess my key take aways are:
- when reaching for a hash function, I used to always start with fnv because of how simple it is.
  I might start with xxh3 now if the hash loop is really hot, since the go implementation seems pretty good.
- in really hot loops, bounds checking matters and I haven't found a good way to eliminate them without resorting to `unsafe`, which is a bit unfortunate.
  But really, this is only needed in the hottest loops.
  Looking at the assembly with `go tool objdump` is a good reflex to have to such loops.
- Inlining can be key to performance.
  One can verify why a function is not inlined by passing `-gcflags="-m=2"` to `go test` or `go build`.
  Fiddling with the functions to their complexity budget fits under the limit (80 as of go 1.24) is tricky.
- set `/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor` to `performance` when testing for more uniform results.
- when working with a 13gb dataset, make sure firefox is not taking 20gb of ram on a 32gb machine or performance is going to be suboptimal.
  (aka monitor pagecache hit ratios with [cachestat](https://github.com/brendangregg/perf-tools/blob/master/fs/cachestat), or io with iostat)
- I should really take more notes *while I work and try things* instead of after the fact.
  It would make the process of writing a summary so much easier and less of a git archeological expedition.

That's all for now!

