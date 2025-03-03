package brc

import (
	"fmt"
	"io"

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
