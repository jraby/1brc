package brc

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"syscall"

	"golang.org/x/exp/mmap"
)

func NewMmapReader(inputFile string) (io.Reader, error) {
	mm, err := mmap.Open(inputFile)
	if err != nil {
		return nil, fmt.Errorf("mmap.Open: %w", err)
	}

	reader := io.NewSectionReader(mm, 0, int64(mm.Len()))

	return reader, nil
}

func NewMmapedSectionReaders(inputFile string, nsections int) ([]*io.SectionReader, error) {
	mm, err := mmap.Open(inputFile)
	if err != nil {
		return nil, fmt.Errorf("mmap.Open: %w", err)
	}

	sectionReaders := make([]*io.SectionReader, nsections)
	mmlen := mm.Len()
	sectionSize := mmlen / nsections
	sectionStartPos := 0
	// log.Printf("len: %d, sectionSize: %d", mmlen, sectionSize)
	for i := range nsections {
		for j := min(sectionStartPos+sectionSize, mmlen-1); j < mmlen; j++ {
			if mm.At(j) == '\n' {
				// log.Printf("start: %10d, len: %10d, end: %10d", sectionStartPos, int64(j-sectionStartPos), j)
				sectionReaders[i] = io.NewSectionReader(mm, int64(sectionStartPos), int64(j-sectionStartPos))
				sectionStartPos = j + 1
				break
			}
		}
	}

	return sectionReaders, nil
}

func NewMmapedSectionReadersMadv(inputFile string, nsections int) ([]*io.SectionReader, error) {
	f, err := os.Open(inputFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := int(fi.Size())
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	err = syscall.Madvise(data, syscall.MADV_SEQUENTIAL|syscall.MADV_WILLNEED)
	if err != nil {
		return nil, err
	}

	r := bytes.NewReader(data)

	sectionReaders := make([]*io.SectionReader, nsections)
	sectionSize := size / nsections
	sectionStartPos := 0
	// log.Printf("len: %d, sectionSize: %d", mmlen, sectionSize)
	for i := range nsections {
		for j := min(sectionStartPos+sectionSize, size-1); j < size; j++ {
			if data[j] == '\n' {
				// log.Printf("start: %10d, len: %10d, end: %10d", sectionStartPos, int64(j-sectionStartPos), j)
				sectionReaders[i] = io.NewSectionReader(r, int64(sectionStartPos), int64(j-sectionStartPos))
				sectionStartPos = j + 1
				break
			}
		}
	}

	return sectionReaders, nil
}
