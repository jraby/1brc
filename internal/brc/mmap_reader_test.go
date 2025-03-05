package brc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMmapedSectionReaders(t *testing.T) {
	rs, err := NewMmapedSectionReaders("../../data/1b.txt", 8)
	assert.NoError(t, err)
	assert.Len(t, rs, 8)
	for _, r := range rs {
		assert.NotNil(t, r)
	}
}
