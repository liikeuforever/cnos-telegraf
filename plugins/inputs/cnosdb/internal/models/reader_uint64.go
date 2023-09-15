package models

import (
	"errors"
)

type UintReader struct {
	columName  string
	columnType ColumnType
	values     Values
	bitset     BitSet
	row        int
	numRows    int
	i          int
	len        int
}

func NewUintReader(column *Column, numRows int) (*UintReader, error) {
	var values Values
	if column.ColValues(&values) == nil {
		return nil, errors.New("col_values is null")
	}

	return &UintReader{
		columName:  string(column.Name()),
		columnType: column.ColumnType(),
		values:     values,
		bitset:     NewBitSet(column.NullbitsBytes(), numRows),
		row:        0,
		numRows:    numRows,
		i:          0,
		len:        values.UintValueLength(),
	}, nil
}

func (r *UintReader) ColumnName() string {
	return r.columName
}

func (r *UintReader) ColumnType() ColumnType {
	return r.columnType
}

func (r *UintReader) Next() (*uint64, bool) {
	r.row += 1
	if r.row >= r.numRows {
		return nil, false
	}
	if !r.bitset.Get(r.row) {
		return nil, true
	}

	r.i += 1
	if r.i > r.len {
		return nil, false
	}
	v := r.values.UintValue(r.i)
	return &v, true
}
