package models

type BitSet struct {
	buf []byte
	len int
}

func NewBitSet(data []byte, num int) BitSet {
	return BitSet{
		buf: data,
		len: num,
	}
}

func (b *BitSet) Get(idx int) bool {
	byteI := idx >> 3
	bitI := idx & 7
	return (b.buf[byteI]>>bitI)&1 != 0
}
