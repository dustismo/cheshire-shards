package proxy

import (
	"crypto/md5"
	"io"
)

// This hashes a the input string into an int between 0 (inclusive) and max (exclusive)
type Hasher interface {
	Hash(input string, max int) (int, error)
}

type DefaultHasher struct {
}

func (this *DefaultHasher) Hash(input string, max int) (int, error) {
	h := md5.New()
	io.WriteString(h, input)
	bytes := h.Sum(nil)
	var hashvalue uint64 = 0
	for i := 0; i <= 3; i++ {
		b := bytes[(len(bytes) - 1 - (3 - i))]
		hashvalue = hashvalue + (uint64(b))<<uint64((3-i)*8)
	}
	return int(hashvalue % uint64(max)), nil
}
