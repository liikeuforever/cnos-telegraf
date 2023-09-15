package models

import (
	"errors"
)

type BoolReader struct {
	columName  string
	columnType ColumnType
	values     Values
	bitset     BitSet
	row        int
	numRows    int
	i          int
	len        int
}

func NewBoolReader(column *Column, numRows int) (*BoolReader, error) {
	var values Values
	if column.ColValues(&values) == nil {
		return nil, errors.New("col_values is null")
	}

	return &BoolReader{
		columName:  string(column.Name()),
		columnType: column.ColumnType(),
		values:     values,
		bitset:     NewBitSet(column.NullbitsBytes(), numRows),
		row:        0,
		numRows:    numRows,
		i:          0,
		len:        values.BoolValueLength(),
	}, nil
}

func (r *BoolReader) ColumnName() string {
	return r.columName
}

func (r *BoolReader) ColumnType() ColumnType {
	return r.columnType
}

func (r *BoolReader) Next() (*bool, bool) {
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
	v := r.values.BoolValue(r.i)
	return &v, true
}
