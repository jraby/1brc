package brc

import (
	"fmt"
	"strings"

	"github.com/cespare/xxhash/v2"
)

type StringHashTable struct {
	buckets      []*Entry
	nbuckets     uint64
	knownEntries []string
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

func (t *StringHashTable) getOrCreate(name string) *Station {
	// h := stringHash(name) % BUCKETS
	// h := stringHash(name) & (t.nbuckets - 1)
	h := xxhash.Sum64String(name) & (t.nbuckets - 1)

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
	}, nil
}

type EntryInt16Station struct {
	name    string
	station *StationInt16
	next    *EntryInt16Station
}

func (t *StringHashTableInt16Stations) getOrCreate(name string) *StationInt16 {
	// h := stringHash(name) % BUCKETS
	// h := stringHash(name) & (t.nbuckets - 1)
	h := xxhash.Sum64String(name) & (t.nbuckets - 1)

	for e := t.buckets[h]; e != nil; e = e.next {
		if e.name == name {
			return e.station
		}
	}

	name = strings.Clone(name)
	// Not found, create new
	newEntry := &EntryInt16Station{
		name:    name,
		station: &StationInt16{},
	}

	newEntry.next = t.buckets[h]
	t.buckets[h] = newEntry
	t.knownEntries = append(t.knownEntries, name)
	return newEntry.station
}

func (t *StringHashTableInt16Stations) KnownEntries() []string {
	return t.knownEntries
}
