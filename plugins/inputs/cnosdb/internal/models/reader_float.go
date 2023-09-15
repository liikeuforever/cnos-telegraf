package models

import (
	"errors"
)

type FloatReader struct {
	columName  string
	columnType ColumnType
	values     Values
	bitset     BitSet
	row        int
	numRows    int
	i          int
	len        int
}

func NewFloatReader(column *Column, numRows int) (*FloatReader, error) {
	var values Values
	if column.ColValues(&values) == nil {
		return nil, errors.New("col_values is null")
	}

	return &FloatReader{
		columName:  string(column.Name()),
		columnType: column.ColumnType(),
		values:     values,
		bitset:     NewBitSet(column.NullbitsBytes(), numRows),
		row:        0,
		numRows:    numRows,
		i:          0,
		len:        values.FloatValueLength(),
	}, nil
}

func (r *FloatReader) ColumnName() string {
	return r.columName
}

func (r *FloatReader) ColumnType() ColumnType {
	return r.columnType
}

func (r *FloatReader) Next() (*float64, bool) {
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
	v := r.values.FloatValue(r.i)
	return &v, true
}
