package bloomfilter

import (
	"math"

	"github.com/adk2004/goDB/db/types"
	"github.com/spaolacci/murmur3"
)

type BloomFilter interface {
	Add(key types.Key)
	Contains(key types.Key) bool
	Clear()
}

type bloomFilter struct {
	bits  []byte
	m     uint // size of bit array
	hashK uint
}

func NewBloom(n uint) BloomFilter {
	p := 1
	m, k := estimateSize(n,float64(p))
	return &bloomFilter{
		bits: make([]byte, (m+7)/8),
		m: m,
		hashK: k,
	}
}

func (bf *bloomFilter) Add(key types.Key) {
	h1 := murmur3.Sum64([]byte(key))
	h2 := murmur3.Sum64WithSeed([]byte(key), 0x9747b28c)

	for i := uint64(0); i < uint64(bf.hashK); i++ {
		idx := (h1 + i*h2) % uint64(bf.m)
		bf.setBit(idx)
	}
}

func (bf *bloomFilter) Contains(key types.Key) bool {
	h1 := murmur3.Sum64([]byte(key))
	h2 := murmur3.Sum64WithSeed([]byte(key), 0x9747b28c)
	for i:= uint64(0); i < uint64(bf.hashK) ; i++ {
		idx := (h1 + i*h2) % uint64(bf.m)
		if(!bf.getBit(idx)) {
			return false;
		}
	}
	return true
}

func (bf *bloomFilter) Clear() {
	bf.bits = make([]byte, (bf.m+7)/8)
}

func (b *bloomFilter) setBit(i uint64) {
	byteIndex := i/8
	bitIndex := i%8
	b.bits[byteIndex] |= 1 << bitIndex
}

func (b *bloomFilter) getBit(i uint64) bool {
	byteIndex := i/8;
	bitIndex := i%8;
	return b.bits[byteIndex] & (1<< bitIndex) != 1
}


func estimateSize(n uint, p float64) (uint, uint) {
	m := -float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)
	k :=  (float64(m) / float64(n)) * math.Ln2
	return uint(m), uint(k)
}
