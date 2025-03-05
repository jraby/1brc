package brc

import (
	"bytes"
	"io"
	"log"
	"sync"
)

const chunksize = 64 * 1024

type chunkPool struct {
	p sync.Pool
}

func (cp *chunkPool) Get() *[]byte {
	b := cp.p.Get().(*[]byte)
	*b = (*b)[:0] // reset
	return b
}

func (cp *chunkPool) Put(b *[]byte) {
	cp.p.Put(b)
}

var ChunkPool = chunkPool{
	p: sync.Pool{
		New: func() any {
			b := make([]byte, 0, chunksize)
			return &b
		},
	},
}

func chunker(r io.Reader, nreaders int) <-chan *[]byte {
	ch := make(chan *[]byte, nreaders*8)

	go func() {
		leftovers := make([]byte, 0, 256)
		for {
			chunk := ChunkPool.Get()
			*chunk = append(*chunk, leftovers...)

			*chunk = (*chunk)[:cap(*chunk)] // extend to use all cap
			n, err := r.Read((*chunk)[len(leftovers):])
			if err != nil {
				if err == io.EOF {
					// log.Printf("EOF: n: %d", n)
					if len(leftovers) > 0 {
						// we didn't read anything, got eof, if we had leftovers, push them out
						*chunk = (*chunk)[:len(leftovers)]
						if (*chunk)[len(*chunk)-1] != '\n' {
							// last line might not have a \n, make sure it does
							*chunk = append(*chunk, '\n')
						}
						ch <- chunk
					}
					close(ch)
					return
				}
				log.Fatalf("Read: %s", err)
			}

			// log.Printf("chunker chunk: %s", string(*chunk))
			lastnl := bytes.LastIndexByte((*chunk)[:len(leftovers)+n], '\n')
			// log.Printf("lastnl: %d, n: %d, leftovers: %d", lastnl, n, len(leftovers))
			if lastnl == -1 {
				log.Fatal("chunker LastIndexByte: garbage input, couldn't find \\n")
			}

			if lastnl < len(leftovers)+n-1 {
				// log.Printf("lastnl: %d, n: %d,  chunksize: %d, leftovers: %d", lastnl, n, len(*chunk), len(leftovers))
				leftovers = leftovers[:0]
				leftovers = append(leftovers, (*chunk)[lastnl+1:]...)
			} else {
				leftovers = leftovers[:0]
			}

			*chunk = (*chunk)[:lastnl+1]
			ch <- chunk
		}
	}()
	return ch
}
