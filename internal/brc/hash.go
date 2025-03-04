package brc

import (
	"fmt"
	"strings"
)

type StringHashTable struct {
	buckets      []*Entry
	nbuckets     uint32
	knownEntries []string
}

func NewStringHashTable(nbuckets uint32) (*StringHashTable, error) {
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
	h := stringHash(name) & (t.nbuckets - 1)

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
