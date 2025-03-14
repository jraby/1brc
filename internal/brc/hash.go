package brc

import (
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"strings"
	"unsafe"
)

type StringHashTable struct {
	buckets      []*Entry
	nbuckets     uint64
	knownEntries []string
	hasher       hash.Hash64
}

func NewStringHashTable(nbuckets uint64) (*StringHashTable, error) {
	// http://www.graphics.stanford.edu/~seander/bithacks.html#DetermineIfPowerOf2
	if nbuckets == 0 || (nbuckets&(nbuckets-1)) != 0 {
		return nil, fmt.Errorf("nbuckets must be a power of 2: %d", nbuckets)
	}
	return &StringHashTable{
		buckets:      make([]*Entry, nbuckets),
		nbuckets:     nbuckets,
		knownEntries: make([]string, 0, nbuckets),
		hasher:       fnv.New64(),
	}, nil
}

type Entry struct {
	name    string
	station *Station
	next    *Entry
}

// fnv-1a
func stringHash(s string) uint32 {
	const prime32 = uint32(16777619)
	hash := uint32(2166136261)

	i := 0
	length := len(s)

	// Process 4 bytes at a time
	for ; i+3 < length; i += 4 {
		hash ^= uint32(s[i])
		hash *= prime32
		hash ^= uint32(s[i+1])
		hash *= prime32
		hash ^= uint32(s[i+2])
		hash *= prime32
		hash ^= uint32(s[i+3])
		hash *= prime32
	}

	// Process remaining bytes
	for ; i < length; i++ {
		hash ^= uint32(s[i])
		hash *= prime32
	}

	return hash
}

// unrolled fnv1a
func byteHash(b []byte) uint32 {
	const prime32 = uint32(16777619)
	hash := uint32(2166136261)

	i := 0
	length := len(b)

	// Process 4 bytes at a time
	for ; i+3 < length; i += 4 {
		hash ^= uint32(b[i])
		hash *= prime32
		hash ^= uint32(b[i+1])
		hash *= prime32
		hash ^= uint32(b[i+2])
		hash *= prime32
		hash ^= uint32(b[i+3])
		hash *= prime32
	}

	// Process remaining bytes
	for ; i < length; i++ {
		hash ^= uint32(b[i])
		hash *= prime32
	}

	return hash
}

func ByteHashBCE(b []byte) uint32 {
	const prime32 = uint32(16777619)
	hash := uint32(2166136261)

	var i int
	length := len(b)

	// bp := unsafe.Pointer(unsafe.SliceData(b))
	bp := unsafe.Pointer(unsafe.SliceData(b))
	// Process 4 bytes at a time

	// log.Printf("uint32(b[0]): %v", uint32(b[0]))
	// log.Printf(" %v", uint32(*(*byte)(unsafe.Add(bp, 0))))
	// log.Printf(" %v", *(*byte)(bp))
	for i = 0; i+3 <= length-1; i += 4 {
		hash ^= uint32(*(*byte)(unsafe.Add(bp, i)))
		hash *= prime32
		hash ^= uint32(*(*byte)(unsafe.Add(bp, i+1)))
		hash *= prime32
		hash ^= uint32(*(*byte)(unsafe.Add(bp, i+2)))
		hash *= prime32
		hash ^= uint32(*(*byte)(unsafe.Add(bp, i+3)))
		hash *= prime32
	}

	// Process remaining bytes
	for ; i <= length-1; i++ {
		// hash ^= uint32(b[i])
		hash ^= uint32(*(*byte)(unsafe.Add(bp, i)))
		hash *= prime32
	}

	// so this does 1 bound check, but jumps a lot so it is slower
	//_ = b[length-1]
	//// Process remaining bytes
	//for i := 0; i <= length-1; i++ {
	//	hash ^= uint32(b[i])
	//	hash *= prime32
	//}

	return hash
}

func ByteHashBCE2Unrolls(b []byte) uint32 {
	const prime32 = uint32(16777619)
	hash := uint32(2166136261)

	var i int
	length := len(b)

	bp := unsafe.Pointer(unsafe.SliceData(b))
	// Process 4 bytes at a time

	for i = 0; i+3 <= length-1; i += 4 {
		hash ^= uint32(*(*byte)(unsafe.Add(bp, i)))
		hash *= prime32
		hash ^= uint32(*(*byte)(unsafe.Add(bp, i+1)))
		hash *= prime32
		hash ^= uint32(*(*byte)(unsafe.Add(bp, i+2)))
		hash *= prime32
		hash ^= uint32(*(*byte)(unsafe.Add(bp, i+3)))
		hash *= prime32
	}
	log.Printf("hash: %b", hash)

	// Process remaining bytes
	for ; i <= length-1; i++ {
		// hash ^= uint32(b[i])
		hash ^= uint32(*(*byte)(unsafe.Add(bp, i)))
		hash *= prime32
	}

	// so this does 1 bound check, but jumps a lot so it is slower
	//_ = b[length-1]
	//// Process remaining bytes
	//for i := 0; i <= length-1; i++ {
	//	hash ^= uint32(b[i])
	//	hash *= prime32
	//}

	return hash
}

func fnv1a(b []byte) uint32 {
	const prime32 = uint32(16777619)
	hash := uint32(2166136261)

	length := len(b)
	// Process remaining bytes
	for i := 0; i < length; i++ {
		hash ^= uint32(b[i])
		hash *= prime32
	}

	return hash
}

func fnv1aRangeIndex(b []byte) uint32 {
	const prime32 = uint32(16777619)
	hash := uint32(2166136261)

	for i := range b {
		hash ^= uint32(b[i])
		hash *= prime32
	}

	return hash
}

func fnv1aRange(bs []byte) uint32 {
	const prime32 = uint32(16777619)
	hash := uint32(2166136261)

	for _, b := range bs {
		hash ^= uint32(b)
		hash *= prime32
	}

	return hash
}

func (t *StringHashTable) getOrCreate(name string) *Station {
	// h := stringHash(name) % BUCKETS
	h := stringHash(name) & uint32((t.nbuckets - 1))
	// h := xxhash.Sum64String(name) & (t.nbuckets - 1)
	// t.hasher.Reset()
	// t.hasher.Write([]byte(name))
	// h := t.hasher.Sum64() & (t.nbuckets - 1)

	for e := t.buckets[h]; e != nil; e = e.next {
		if e.name == name {
			return e.station
		}
	}

	name = strings.Clone(name)
	// Not found, create new
	newEntry := &Entry{
		name:    name,
		station: &Station{},
	}

	newEntry.next = t.buckets[h]
	t.buckets[h] = newEntry
	t.knownEntries = append(t.knownEntries, name)
	return newEntry.station
}

func (t *StringHashTable) KnownEntries() []string {
	return t.knownEntries
}

type StringHashTableInt16Stations struct {
	buckets      []*EntryInt16Station
	nbuckets     uint64
	knownEntries []string
	hasher       hash.Hash64
}

func NewStringHashTableInt16Stations(nbuckets uint64) (*StringHashTableInt16Stations, error) {
	// http://www.graphics.stanford.edu/~seander/bithacks.html#DetermineIfPowerOf2
	if nbuckets == 0 || (nbuckets&(nbuckets-1)) != 0 {
		return nil, fmt.Errorf("nbuckets must be a power of 2: %d", nbuckets)
	}
	return &StringHashTableInt16Stations{
		buckets:      make([]*EntryInt16Station, nbuckets),
		nbuckets:     nbuckets,
		knownEntries: make([]string, 0, nbuckets),
		hasher:       fnv.New64(),
	}, nil
}

type EntryInt16Station struct {
	name    string
	station *StationInt16
	next    *EntryInt16Station
}

func (t *StringHashTableInt16Stations) getOrCreate(name []byte) *StationInt16 {
	// h := stringHash(name) % BUCKETS
	h := uint64(byteHash(name)) & (t.nbuckets - 1)
	// h := xxhash.Sum64String(name) & (t.nbuckets - 1)
	// t.hasher.Reset()
	// t.hasher.Write(name)
	// h := t.hasher.Sum64() & (t.nbuckets - 1)

	namestr := unsafe.String(unsafe.SliceData(name), len(name))
	for e := t.buckets[h]; e != nil; e = e.next {
		if e.name == namestr {
			return e.station
		}
	}

	namestr = strings.Clone(namestr)
	// Not found, create new
	newEntry := &EntryInt16Station{
		name:    namestr,
		station: &StationInt16{},
	}

	newEntry.next = t.buckets[h]
	t.buckets[h] = newEntry
	t.knownEntries = append(t.knownEntries, namestr)
	return newEntry.station
}

func (t *StringHashTableInt16Stations) KnownEntries() []string {
	return t.knownEntries
}
