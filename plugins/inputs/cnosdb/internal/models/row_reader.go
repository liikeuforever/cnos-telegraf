package models

import (
	"time"
)

type RowReader struct {
	fieldReaders []interface{}
	tagReaders   []*StringReader
	timeReader   *IntReader
	row          int
	numRows      int
}

func NewRowReader(numRows int, fieldReaders []interface{}, tagReaders []*StringReader, timeReader *IntReader) *RowReader {
	return &RowReader{
		fieldReaders: fieldReaders,
		tagReaders:   tagReaders,
		timeReader:   timeReader,
		row:          0,
		numRows:      numRows,
	}
}

func (r *RowReader) Next() (fields map[string]interface{}, tags map[string]string, ts *time.Time, hasNextRow bool) {
	for _, f := range r.fieldReaders {
		switch reader := f.(type) {
		case FloatReader:
			if v, hasNext := reader.Next(); v != nil {
				fields[reader.ColumnName()] = *v
				if hasNext {
					hasNextRow = true
				}
			}
		case IntReader:
			if v, hasNext := reader.Next(); v != nil {
				fields[reader.ColumnName()] = *v
				if hasNext {
					hasNextRow = true
				}
			}
		case UintReader:
			if v, hasNext := reader.Next(); v != nil {
				fields[reader.ColumnName()] = *v
				if hasNext {
					hasNextRow = true
				}
			}
		case BoolReader:
			if v, hasNext := reader.Next(); v != nil {
				fields[reader.ColumnName()] = *v
				if hasNext {
					hasNextRow = true
				}
			}
		case StringReader:
			if v, hasNext := reader.Next(); v != nil {
				fields[reader.ColumnName()] = *v
				if hasNext {
					hasNextRow = true
				}
			}
		}
	}
	for _, t := range r.tagReaders {
		if v, hasNext := t.Next(); v != nil {
			tags[t.ColumnName()] = *v
			if hasNext {
				hasNextRow = true
			}
		}
	}

	if r.timeReader == nil {
		ts = nil
	} else {
		if v, hasNext := r.timeReader.Next(); v != nil {
			t := time.Unix(*v, *v%1_000_000_000)
			ts = &t
			if hasNext {
				hasNextRow = true
			}
		}
	}

	return
}
