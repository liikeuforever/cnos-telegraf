package models

import (
	"errors"
)

type IntReader struct {
	columName  string
	columnType ColumnType
	values     Values
	bitset     BitSet
	row        int
	numRows    int
	i          int
	len        int
}

func NewIntReader(column *Column, numRows int) (*IntReader, error) {
	var values Values
	if column.ColValues(&values) == nil {
		return nil, errors.New("col_values is null")
	}

	return &IntReader{
		columName:  string(column.Name()),
		columnType: column.ColumnType(),
		values:     values,
		bitset:     NewBitSet(column.NullbitsBytes(), numRows),
		row:        0,
		numRows:    numRows,
		i:          0,
		len:        values.IntValueLength(),
	}, nil
}

func (r *IntReader) ColumnName() string {
	return r.columName
}

func (r *IntReader) ColumnType() ColumnType {
	return r.columnType
}

func (r *IntReader) Next() (*int64, bool) {
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
	v := r.values.IntValue(r.i)
	return &v, true
}
