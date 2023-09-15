package models

import (
	"errors"
)

type StringReader struct {
	columName  string
	columnType ColumnType
	values     Values
	bitset     BitSet
	row        int
	numRows    int
	i          int
	len        int
}

func NewStringReader(column *Column, numRows int) (*StringReader, error) {
	var values Values
	if column.ColValues(&values) == nil {
		return nil, errors.New("col_values is null")
	}

	return &StringReader{
		columName:  string(column.Name()),
		columnType: column.ColumnType(),
		values:     values,
		bitset:     NewBitSet(column.NullbitsBytes(), numRows),
		row:        0,
		numRows:    numRows,
		i:          0,
		len:        values.StringValueLength(),
	}, nil
}

func (r *StringReader) ColumnName() string {
	return r.columName
}

func (r *StringReader) ColumnType() ColumnType {
	return r.columnType
}

func (r *StringReader) Next() (*string, bool) {
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
	v := string(r.values.StringValue(r.i))
	return &v, true
}
