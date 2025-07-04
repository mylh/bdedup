// The MIT License (MIT)
// Copyright (c) 2014 Andreas Briese, eduToolbox@Bri-C GmbH, Sarstedt

// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

// 2019/08/25 code revision to reduce unsafe use
// Parts are adopted from the fork at ipfs/bbloom after performance rev by
// Steve Allen (https://github.com/Stebalien)
// (see https://github.com/ipfs/bbloom/blob/master/bbloom.go)
// -> func Has
// -> func set
// -> func add

// Original author: Andreas Briese https://github.com/AndreasBriese/bbloom

// added methods for binary serialization

package bbloom

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"math"
	"sync"
	"unsafe"
)

// helper
// not needed anymore by Set
// var mask = []uint8{1, 2, 4, 8, 16, 32, 64, 128}

func getSize(ui64 uint64) (size uint64, exponent uint64) {
	if ui64 < uint64(512) {
		ui64 = uint64(512)
	}
	size = uint64(1)
	for size < ui64 {
		size <<= 1
		exponent++
	}
	return size, exponent
}

func calcSizeByWrongPositives(numEntries, wrongs float64) (uint64, uint64) {
	size := -1 * numEntries * math.Log(wrongs) / math.Pow(float64(0.69314718056), 2)
	locs := math.Ceil(float64(0.69314718056) * size / numEntries)
	return uint64(size), uint64(locs)
}

// New
// returns a new bloomfilter
func New(params ...float64) (bloomfilter Bloom) {
	var entries, locs uint64
	if len(params) == 2 {
		if params[1] < 1 {
			entries, locs = calcSizeByWrongPositives(params[0], params[1])
		} else {
			entries, locs = uint64(params[0]), uint64(params[1])
		}
	} else {
		log.Fatal("usage: New(float64(number_of_entries), float64(number_of_hashlocations)) i.e. New(float64(1000), float64(3)) or New(float64(number_of_entries), float64(number_of_hashlocations)) i.e. New(float64(1000), float64(0.03))")
	}
	size, exponent := getSize(uint64(entries))
	bloomfilter = Bloom{
		Mtx:     &sync.Mutex{},
		sizeExp: exponent,
		size:    size - 1,
		setLocs: locs,
		shift:   64 - exponent,
	}
	bloomfilter.Size(size)
	return bloomfilter
}

// NewWithBoolset
// takes a []byte slice and number of locs per entry
// returns the bloomfilter with a bitset populated according to the input []byte
func NewWithBoolset(bs *[]byte, locs uint64) (bloomfilter Bloom) {
	bloomfilter = New(float64(len(*bs)<<3), float64(locs))
	for i, b := range *bs {
		*(*uint8)(unsafe.Pointer(uintptr(unsafe.Pointer(&bloomfilter.bitset[0])) + uintptr(i))) = b
	}
	return bloomfilter
}

// bloomJSONImExport
// Im/Export structure used by JSONMarshal / JSONUnmarshal
type bloomJSONImExport struct {
	FilterSet []byte
	SetLocs   uint64
}

// JSONUnmarshal
// takes JSON-Object (type bloomJSONImExport) as []bytes
// returns Bloom object
func JSONUnmarshal(dbData []byte) Bloom {
	bloomImEx := bloomJSONImExport{}
	json.Unmarshal(dbData, &bloomImEx)
	buf := bytes.NewBuffer(bloomImEx.FilterSet)
	bs := buf.Bytes()
	bf := NewWithBoolset(&bs, bloomImEx.SetLocs)
	return bf
}

// Bloom filter
type Bloom struct {
	Mtx     *sync.Mutex
	ElemNum uint64
	bitset  []uint64
	sizeExp uint64
	size    uint64
	setLocs uint64
	shift   uint64
}

// <--- http://www.cse.yorku.ca/~oz/hash.html
// modified Berkeley DB Hash (32bit)
// hash is casted to l, h = 16bit fragments
// func (bl Bloom) absdbm(b *[]byte) (l, h uint64) {
// 	hash := uint64(len(*b))
// 	for _, c := range *b {
// 		hash = uint64(c) + (hash << 6) + (hash << bl.sizeExp) - hash
// 	}
// 	h = hash >> bl.shift
// 	l = hash << bl.shift >> bl.shift
// 	return l, h
// }

// Update: found sipHash of Jean-Philippe Aumasson & Daniel J. Bernstein to be even faster than absdbm()
// https://131002.net/siphash/
// siphash was implemented for Go by Dmitry Chestnykh https://github.com/dchest/siphash

// Add
// set the bit(s) for entry; Adds an entry to the Bloom filter
func (bl *Bloom) Add(entry []byte) {
	l, h := bl.sipHash(entry)
	for i := uint64(0); i < bl.setLocs; i++ {
		bl.set((h + i*l) & bl.size)
		bl.ElemNum++
	}
}

// AddTS
// Thread safe: Mutex.Lock the bloomfilter for the time of processing the entry
func (bl *Bloom) AddTS(entry []byte) {
	bl.Mtx.Lock()
	defer bl.Mtx.Unlock()
	bl.Add(entry)
}

// Has
// check if bit(s) for entry is/are set
// returns true if the entry was added to the Bloom Filter
func (bl Bloom) Has(entry []byte) bool {
	l, h := bl.sipHash(entry)
	res := true
	for i := uint64(0); i < bl.setLocs; i++ {
		res = res && bl.isSet((h+i*l)&bl.size)
		// https://github.com/ipfs/bbloom/commit/84e8303a9bfb37b2658b85982921d15bbb0fecff
		// // Branching here (early escape) is not worth it
		// // This is my conclusion from benchmarks
		// // (prevents loop unrolling)
		// switch bl.IsSet((h + i*l) & bl.size) {
		// case false:
		// 	return false
		// }
	}
	return res
}

// HasTS
// Thread safe: Mutex.Lock the bloomfilter for the time of processing the entry
func (bl *Bloom) HasTS(entry []byte) bool {
	bl.Mtx.Lock()
	defer bl.Mtx.Unlock()
	return bl.Has(entry)
}

// AddIfNotHas
// Only Add entry if it's not present in the bloomfilter
// returns true if entry was added
// returns false if entry was allready registered in the bloomfilter
func (bl Bloom) AddIfNotHas(entry []byte) (added bool) {
	if bl.Has(entry) {
		return added
	}
	bl.Add(entry)
	return true
}

// AddIfNotHasTS
// Tread safe: Only Add entry if it's not present in the bloomfilter
// returns true if entry was added
// returns false if entry was allready registered in the bloomfilter
func (bl *Bloom) AddIfNotHasTS(entry []byte) (added bool) {
	bl.Mtx.Lock()
	defer bl.Mtx.Unlock()
	return bl.AddIfNotHas(entry)
}

// Size
// make Bloom filter with as bitset of size sz
func (bl *Bloom) Size(sz uint64) {
	bl.bitset = make([]uint64, sz>>6)
}

// Clear
// resets the Bloom filter
func (bl *Bloom) Clear() {
	bs := bl.bitset
	for i := range bs {
		bs[i] = 0
	}
}

// Set
// set the bit[idx] of bitsit
func (bl *Bloom) set(idx uint64) {
	// ommit unsafe
	// 	*(*uint8)(unsafe.Pointer(uintptr(unsafe.Pointer(&bl.bitset[idx>>6])) + uintptr((idx%64)>>3))) |= mask[idx%8]
	bl.bitset[idx>>6] |= 1 << (idx % 64)
}

// IsSet
// check if bit[idx] of bitset is set
// returns true/false
func (bl *Bloom) isSet(idx uint64) bool {
	// ommit unsafe
	// return (((*(*uint8)(unsafe.Pointer(uintptr(unsafe.Pointer(&bl.bitset[idx>>6])) + uintptr((idx%64)>>3)))) >> (idx % 8)) & 1) == 1
	return bl.bitset[idx>>6]&(1<<(idx%64)) != 0
}

// JSONMarshal
// returns JSON-object (type bloomJSONImExport) as []byte
func (bl Bloom) JSONMarshal() []byte {
	bloomImEx := bloomJSONImExport{}
	bloomImEx.SetLocs = uint64(bl.setLocs)
	bloomImEx.FilterSet = make([]byte, len(bl.bitset)<<3)
	for i := range bloomImEx.FilterSet {
		bloomImEx.FilterSet[i] = *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&bl.bitset[0])) + uintptr(i)))
	}
	data, err := json.Marshal(bloomImEx)
	if err != nil {
		log.Fatal("json.Marshal failed: ", err)
	}
	return data
}

// // alternative hashFn
// func (bl Bloom) fnv64a(b *[]byte) (l, h uint64) {
// 	h64 := fnv.New64a()
// 	h64.Write(*b)
// 	hash := h64.Sum64()
// 	h = hash >> 32
// 	l = hash << 32 >> 32
// 	return l, h
// }
//
// // <-- http://partow.net/programming/hashfunctions/index.html
// // citation: An algorithm proposed by Donald E. Knuth in The Art Of Computer Programming Volume 3,
// // under the topic of sorting and search chapter 6.4.
// // modified to fit with boolset-length
// func (bl Bloom) DEKHash(b *[]byte) (l, h uint64) {
// 	hash := uint64(len(*b))
// 	for _, c := range *b {
// 		hash = ((hash << 5) ^ (hash >> bl.shift)) ^ uint64(c)
// 	}
// 	h = hash >> bl.shift
// 	l = hash << bl.sizeExp >> bl.sizeExp
// 	return l, h
// }

// BinaryMarshal serializes the Bloom filter to a writer in binary format.
func (bl *Bloom) BinaryMarshal(w io.Writer) error {
	// Save main config fields
	// Order: sizeExp, size, setLocs, shift, ElemNum, bitset length, then bitset
	if err := binary.Write(w, binary.LittleEndian, bl.sizeExp); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, bl.size); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, bl.setLocs); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, bl.shift); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, bl.ElemNum); err != nil {
		return err
	}
	length := uint64(len(bl.bitset))
	if err := binary.Write(w, binary.LittleEndian, length); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, bl.bitset)
}

// BinaryUnmarshal deserializes the Bloom filter from a reader in binary format.
// Returns a value, not a pointer, to match your API style.
func BinaryUnmarshal(r io.Reader) (Bloom, error) {
	bl := Bloom{
		Mtx: &sync.Mutex{},
	}
	if err := binary.Read(r, binary.LittleEndian, &bl.sizeExp); err != nil {
		return bl, err
	}
	if err := binary.Read(r, binary.LittleEndian, &bl.size); err != nil {
		return bl, err
	}
	if err := binary.Read(r, binary.LittleEndian, &bl.setLocs); err != nil {
		return bl, err
	}
	if err := binary.Read(r, binary.LittleEndian, &bl.shift); err != nil {
		return bl, err
	}
	if err := binary.Read(r, binary.LittleEndian, &bl.ElemNum); err != nil {
		return bl, err
	}
	var length uint64
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return bl, err
	}
	bl.bitset = make([]uint64, length)
	if err := binary.Read(r, binary.LittleEndian, bl.bitset); err != nil {
		return bl, err
	}
	return bl, nil
}
