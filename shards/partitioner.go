package main

import "fmt"
import "crypto/md5"
import "io"

type Partitioner struct {
	NumPartitions int
}

func (p Partitioner) partition(bytes []byte, numparts int) int {
	var hashvalue uint64 = 0
	for i := 0; i <= 3; i++ {
		b := bytes[(len(bytes) - 1 - (3 - i))]
		hashvalue = hashvalue + (uint64(b))<<uint64((3-i)*8)
	}
	return int(hashvalue) % numparts
}

func getMd5(str string) []byte {
	h := md5.New()
	io.WriteString(h, str)
	return h.Sum(nil)
}

func main() {
	p := Partitioner{1000000}
	fmt.Println(p.partition(getMd5("s3asdfastest"), p.NumPartitions))
}
