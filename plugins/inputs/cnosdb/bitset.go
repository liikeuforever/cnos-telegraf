package cnosdb

type BitSet struct {
	buf []byte
	len int
}

func (self *BitSet) get(idx int) bool {
	byte_idx := idx >> 3
	bit_idx := idx & 7
	return (self.buf[byte_idx]>>bit_idx)&1 != 0
}
