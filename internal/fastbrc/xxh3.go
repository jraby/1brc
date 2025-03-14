package fastbrc

import (
	"encoding/binary"
	"math/bits"
	"unsafe"
)

/*
BSD 2-Clause License

Copyright (c) 2012-2014, Yann Collet
Copyright (c) 2019, Jeff Wendling
All rights reserved.

xxHash Library

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice, this
  list of conditions and the following disclaimer in the documentation and/or
  other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

const (
	_stripe = 64
	_block  = 1024

	prime32_1 = 2654435761
	prime32_2 = 2246822519
	prime32_3 = 3266489917

	prime64_1 = 11400714785074694791
	prime64_2 = 14029467366897019727
	prime64_3 = 1609587929392839161
	prime64_4 = 9650029242287828579
	prime64_5 = 2870177450012600261
)

var key = ptr(&[...]u8{
	0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe /* 8   */, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c, /* 16  */
	0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb /* 24  */, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f, /* 32  */
	0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78 /* 40  */, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21, /* 48  */
	0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e /* 56  */, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c, /* 64  */
	0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb /* 72  */, 0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3, /* 80  */
	0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e /* 88  */, 0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8, /* 96  */
	0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f /* 104 */, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d, /* 112 */
	0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31 /* 120 */, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64, /* 128 */
	0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3 /* 136 */, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb, /* 144 */
	0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49 /* 152 */, 0xd3, 0x16, 0x55, 0x26, 0x29, 0xd4, 0x68, 0x9e, /* 160 */
	0x2b, 0x16, 0xbe, 0x58, 0x7d, 0x47, 0xa1, 0xfc /* 168 */, 0x8f, 0xf8, 0xb8, 0xd1, 0x7a, 0xd0, 0x31, 0xce, /* 176 */
	0x45, 0xcb, 0x3a, 0x8f, 0x95, 0x16, 0x04, 0x28 /* 184 */, 0xaf, 0xd7, 0xfb, 0xca, 0xbb, 0x4b, 0x40, 0x7e, /* 192 */
})

const (
	key64_000 u64 = 0xbe4ba423396cfeb8
	key64_008 u64 = 0x1cad21f72c81017c
	key64_016 u64 = 0xdb979083e96dd4de
	key64_024 u64 = 0x1f67b3b7a4a44072
	key64_032 u64 = 0x78e5c0cc4ee679cb
	key64_040 u64 = 0x2172ffcc7dd05a82
	key64_048 u64 = 0x8e2443f7744608b8
	key64_056 u64 = 0x4c263a81e69035e0
	key64_064 u64 = 0xcb00c391bb52283c
	key64_072 u64 = 0xa32e531b8b65d088
	key64_080 u64 = 0x4ef90da297486471
	key64_088 u64 = 0xd8acdea946ef1938
	key64_096 u64 = 0x3f349ce33f76faa8
	key64_104 u64 = 0x1d4f0bc7c7bbdcf9
	key64_112 u64 = 0x3159b4cd4be0518a
	key64_120 u64 = 0x647378d9c97e9fc8
	key64_128 u64 = 0xc3ebd33483acc5ea
	key64_136 u64 = 0xeb6313faffa081c5
	key64_144 u64 = 0x49daf0b751dd0d17
	key64_152 u64 = 0x9e68d429265516d3
	key64_160 u64 = 0xfca1477d58be162b
	key64_168 u64 = 0xce31d07ad1b8f88f
	key64_176 u64 = 0x280416958f3acb45
	key64_184 u64 = 0x7e404bbbcafbd7af

	key64_103 u64 = 0x4f0bc7c7bbdcf93f
	key64_111 u64 = 0x59b4cd4be0518a1d
	key64_119 u64 = 0x7378d9c97e9fc831
	key64_127 u64 = 0xebd33483acc5ea64

	key64_121 u64 = 0xea647378d9c97e9f
	key64_129 u64 = 0xc5c3ebd33483acc5
	key64_137 u64 = 0x17eb6313faffa081
	key64_145 u64 = 0xd349daf0b751dd0d
	key64_153 u64 = 0x2b9e68d429265516
	key64_161 u64 = 0x8ffca1477d58be16
	key64_169 u64 = 0x45ce31d07ad1b8f8
	key64_177 u64 = 0xaf280416958f3acb

	key64_011 = 0x6dd4de1cad21f72c
	key64_019 = 0xa44072db979083e9
	key64_027 = 0xe679cb1f67b3b7a4
	key64_035 = 0xd05a8278e5c0cc4e
	key64_043 = 0x4608b82172ffcc7d
	key64_051 = 0x9035e08e2443f774
	key64_059 = 0x52283c4c263a81e6
	key64_067 = 0x65d088cb00c391bb

	key64_117 = 0xd9c97e9fc83159b4
	key64_125 = 0x3483acc5ea647378
	key64_133 = 0xfaffa081c5c3ebd3
	key64_141 = 0xb751dd0d17eb6313
	key64_149 = 0x29265516d349daf0
	key64_157 = 0x7d58be162b9e68d4
	key64_165 = 0x7ad1b8f88ffca147
	key64_173 = 0x958f3acb45ce31d0
)

const (
	key32_000 u32 = 0xbe4ba423
	key32_004 u32 = 0x396cfeb8
	key32_008 u32 = 0x1cad21f7
	key32_012 u32 = 0x2c81017c
)

// Uint128 is a 128 bit value.
// The actual value can be thought of as u.Hi<<64 | u.Lo.
type Uint128 struct {
	Hi, Lo uint64
}

type (
	ptr = unsafe.Pointer
	ui  = uintptr

	u8   = uint8
	u32  = uint32
	u64  = uint64
	u128 = Uint128
)

type str struct {
	p ptr
	l uint
}

func readU8(p ptr, o ui) uint8 {
	return *(*uint8)(ptr(ui(p) + o))
}

func readU16(p ptr, o ui) uint16 {
	b := (*[2]byte)(ptr(ui(p) + o))
	return uint16(b[0]) | uint16(b[1])<<8
}

func readU32(p ptr, o ui) uint32 {
	b := (*[4]byte)(ptr(ui(p) + o))
	return binary.LittleEndian.Uint32(b[:])
}

func readU64(p ptr, o ui) uint64 {
	b := (*[8]byte)(ptr(ui(p) + o))
	return binary.LittleEndian.Uint64(b[:])
}

func xxh64AvalancheSmall(x u64) u64 {
	// x ^= x >> 33                    // x must be < 32 bits
	// x ^= u64(key32_000 ^ key32_004) // caller must do this
	x *= prime64_2
	x ^= x >> 29
	x *= prime64_3
	x ^= x >> 32
	return x
}

func xxhAvalancheSmall(x u64) u64 {
	x ^= x >> 33
	x *= prime64_2
	x ^= x >> 29
	x *= prime64_3
	x ^= x >> 32
	return x
}

func xxh64AvalancheFull(x u64) u64 {
	x ^= x >> 33
	x *= prime64_2
	x ^= x >> 29
	x *= prime64_3
	x ^= x >> 32
	return x
}

func xxh3Avalanche(x u64) u64 {
	x ^= x >> 37
	x *= 0x165667919e3779f9
	x ^= x >> 32
	return x
}

func rrmxmx(h64 u64, len u64) u64 {
	h64 ^= bits.RotateLeft64(h64, 49) ^ bits.RotateLeft64(h64, 24)
	h64 *= 0x9fb21c651e98df25
	h64 ^= (h64 >> 35) + len
	h64 *= 0x9fb21c651e98df25
	h64 ^= (h64 >> 28)
	return h64
}

func mulFold64(x, y u64) u64 {
	hi, lo := bits.Mul64(x, y)
	return hi ^ lo
}
