package brc

import (
	"bytes"
	"testing"
	"unsafe"

	radix "github.com/hashicorp/go-immutable-radix/v2"
	artv2 "github.com/plar/go-adaptive-radix-tree/v2"
)

// well this allocates way too much it can't compete
// BenchmarkStationFindRadix-8                 8878            637546 ns/op          699902 B/op      10092 allocs/op
func BenchmarkStationFindRadix(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rx := radix.New[*StationInt16]()
		for range 8 {
			for _, name := range names {
				s, found := rx.Get(name)
				if !found {
					s = &StationInt16{Name: bytes.Clone(name)}
					rx, _, _ = rx.Insert(name, s)
				}
				_ = s
			}
		}
	}
}

// BenchmarkStationFindStdMap-8               54958            109263 ns/op           56072 B/op       1242 allocs/op
func BenchmarkStationFindStdMap(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stationByName := make(map[string]*StationInt16, 512)
		for range 8 {
			for _, name := range names {
				s, found := stationByName[string(name)]
				if !found {
					s = &StationInt16{Name: bytes.Clone(name)}
					stationByName[string(name)] = s
				}
				_ = s
			}
		}
		b.StopTimer()
		clear(stationByName)
		b.StartTimer()
	}
}

func BenchmarkStationFindStdMapPreallocTargets(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stationTable := make([]StationInt16, 512)
		stationTableNextIdx := 0
		stationByName := make(map[string]*StationInt16, 512)
		for range 8 {
			for _, name := range names {
				s, found := stationByName[string(name)]
				if !found {
					stationTable[stationTableNextIdx].Name = bytes.Clone(name)
					stationByName[string(name)] = &stationTable[stationTableNextIdx]
					stationTableNextIdx++
				}
				_ = s
			}
		}
		b.StopTimer()
		clear(stationByName)
		b.StartTimer()
	}
}

// this doesn't support collisions but it is much faster than stdmap
// BenchmarkStationFindBigArrayFnv1a-8       154029             39492 ns/op            4489 B/op        413 allocs/op
func BenchmarkStationFindBigArrayFnv1a(b *testing.B) {
	b.ReportAllocs()
	stationTable := make([]StationInt16, 65535)
	for i := 0; i < b.N; i++ {
		for range 8 {
			for _, name := range names {
				h := ByteHashBCE(name) % uint32(len(stationTable))
				s := &stationTable[h]
				if len(s.Name) == 0 {
					s.Name = bytes.Clone(name)
					//} else {
					//	if !bytes.Equal(s.Name, name) {
					//		panic("woupelai")
					//	}
				}
				_ = s
			}
		}
		b.StopTimer()
		for i := range stationTable {
			stationTable[i] = StationInt16{}
		}
		b.StartTimer()
	}
}

func BenchmarkStationFindBigArrayFnv1aCollisionCheck(b *testing.B) {
	b.ReportAllocs()
	stationTable := make([]StationInt16, 65535)
	for i := 0; i < b.N; i++ {
		for range 8 {
			for _, name := range names {
				h := ByteHashBCE(name) % uint32(len(stationTable))
				s := &stationTable[h]
				if len(s.Name) == 0 {
					s.Name = bytes.Clone(name)
				} else {
					if !bytes.Equal(s.Name, name) {
						panic("woupelai")
					}
				}
				_ = s
			}
		}
		b.StopTimer()
		for i := range stationTable {
			stationTable[i] = StationInt16{}
		}
		b.StartTimer()
	}
}

func BenchmarkStationFindBigArrayFnv1aCollisionCheckUnsafeUnrolled(b *testing.B) {
	b.ReportAllocs()
	stationTable := make([]StationInt16, 65535)
	for i := 0; i < b.N; i++ {
		for range 8 {
			for _, name := range names {
				h := ByteHashBCE(name) % uint32(len(stationTable))
				s := &stationTable[h]
				if len(s.Name) == 0 {
					s.Name = bytes.Clone(name)
				} else {
					namelen := len(name)
					if namelen == len(s.Name) { //||!bytes.Equal(s.Name, name) {
						i := 0
						namep := unsafe.Pointer(unsafe.SliceData(name))
						snamep := unsafe.Pointer(unsafe.SliceData(s.Name))
						for ; i <= namelen-1-3; i += 4 {
							if *(*uint32)(unsafe.Add(namep, i)) != *(*uint32)(unsafe.Add(snamep, i)) {
								panic("woupelai")
							}
						}
						for ; i < len(name)-1; i++ {
							if *(*byte)(unsafe.Add(namep, i)) != *(*byte)(unsafe.Add(snamep, i)) {
								panic("woupelailai")
							}
						}
					}
				}
				_ = s
			}
		}
		b.StopTimer()
		for i := range stationTable {
			stationTable[i] = StationInt16{}
		}
		b.StartTimer()
	}
}

func BenchmarkStationFindArtv2(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stationTable := make([]StationInt16, 512)
		stationTableNextIdx := 0
		tree := artv2.New()
		var s *StationInt16
		for range 8 {
			for _, name := range names {
				v, found := tree.Search(artv2.Key(name))
				if !found {
					s = &stationTable[stationTableNextIdx]
					s.Name = bytes.Clone(name)
					tree.Insert(artv2.Key(name), s)
				} else {
					s = v.(*StationInt16)
				}
				_ = s
			}
		}
		b.StopTimer()
		for i := range stationTable {
			stationTable[i] = StationInt16{}
		}
		b.StartTimer()
	}
}
