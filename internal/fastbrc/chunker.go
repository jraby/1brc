package fastbrc

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

type Chunker struct {
	r       io.Reader
	p       sync.Pool
	chunkCh chan *[]byte
}

func NewChunker(r io.Reader, chCap, chunkSize int) *Chunker {
	return &Chunker{
		r:       r,
		chunkCh: make(chan *[]byte, chCap),
		p: sync.Pool{
			New: func() any {
				b := make([]byte, 0, chunkSize)
				return &b
			},
		},
	}
}

func (c *Chunker) getChunk() *[]byte {
	b := c.p.Get().(*[]byte)
	*b = (*b)[:0] // reset
	return b
}

func (c *Chunker) ReleaseChunk(chunk *[]byte) {
	c.p.Put(chunk)
}

func (c *Chunker) NextChunk() *[]byte {
	return <-c.chunkCh
}

func (c *Chunker) Run() error {
	leftovers := make([]byte, 0, 256)
	for {
		chunk := c.getChunk()
		*chunk = append(*chunk, leftovers...) // leftovers at beginning of chunk
		currentReadStartPos := len(leftovers) // keep ref for calculations
		leftovers = leftovers[:0]             // reset
		*chunk = (*chunk)[:cap(*chunk)]       // extend to use all cap

		n, err := c.r.Read((*chunk)[currentReadStartPos:])
		// log.Printf("n: %d, err: %v, currentReadStartPos: %d", n, err, currentReadStartPos)
		if err != nil {
			if err == io.EOF {
				// we didn't read anything, got eof, if we had leftovers, push them out
				if currentReadStartPos > 0 {
					*chunk = (*chunk)[:currentReadStartPos]
					if (*chunk)[len(*chunk)-1] != '\n' {
						// last line might not have a \n, make sure it does
						*chunk = append(*chunk, '\n')
					}
					c.chunkCh <- chunk
				}
				close(c.chunkCh)
				return nil
			}
			return fmt.Errorf("failed Read: %w", err)
		}
		(*chunk) = (*chunk)[:currentReadStartPos+n] // chop at last read to avoid having to calculate it everytime

		lastnl := bytes.LastIndexByte(*chunk, '\n')
		if lastnl == -1 {
			// no \n and not EOF, keep reading
			leftovers = append(leftovers, (*chunk)...)
			// log.Printf("n: %d, err: %v, currentReadStartPos: %d, leftovers: %d", n, err, currentReadStartPos, len(leftovers))
			continue
		}

		if lastnl < currentReadStartPos+n-1 {
			leftovers = append(leftovers, (*chunk)[lastnl+1:]...)
		}

		*chunk = (*chunk)[:lastnl+1]
		c.chunkCh <- chunk
	}
}

type ByteChunker struct {
	b         []byte
	chunkCh   chan *[]byte
	chunkSize int
}

func NewByteChunker(input []byte, chCap, chunkSize int) *ByteChunker {
	return &ByteChunker{
		b:         input,
		chunkCh:   make(chan *[]byte, chCap),
		chunkSize: chunkSize,
	}
}

func (c *ByteChunker) ReleaseChunk(chunk *[]byte) {
	// to satisfy interface
}

func (c *ByteChunker) NextChunk() *[]byte {
	return <-c.chunkCh
}

func (c *ByteChunker) Run() error {
	readStartPos := 0
	for readStartPos < len(c.b) {
		chunk := c.b[readStartPos:min(readStartPos+c.chunkSize, len(c.b))]
		lastnl := bytes.LastIndexByte(chunk, '\n')
		if lastnl == -1 {
			return fmt.Errorf("missing \\n in chunk")
		}

		chunk = chunk[:lastnl+1]   // include \n
		readStartPos += len(chunk) // start next read after \n

		c.chunkCh <- &chunk
		// log.Printf("chunk:\n%s", chunk)
		// log.Printf("lenchunk: %d", len(chunk))
		// log.Printf("readStartPos: %d", readStartPos)
		// log.Printf("lenb: %d", len(c.b))
	}
	close(c.chunkCh)
	return nil
}
