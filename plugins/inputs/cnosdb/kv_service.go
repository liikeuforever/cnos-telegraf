package cnosdb

import (
	"fmt"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs/cnosdb/internal/models"
	"github.com/influxdata/telegraf/plugins/inputs/cnosdb/internal/service"
	"io"
)

//go:generate flatc -o internal/models --go --go-namespace models --gen-onefile ./protos/models/models.fbs

//go install google.golang.org/protobuf/cmd/protoc-gen-go@version
//go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@version
//go:generate protoc --go_out=./internal --go-grpc_out=./internal ./protos/service/kv_service.proto

var _ service.TSKVServiceServer = (*TSKVServiceServerImpl)(nil)

type TSKVServiceServerImpl struct {
	accumulator telegraf.Accumulator

	service.UnimplementedTSKVServiceServer
}

func NewTSKVService(acc telegraf.Accumulator) service.TSKVServiceServer {
	return TSKVServiceServerImpl{
		accumulator: acc,
	}
}

func (s TSKVServiceServerImpl) WritePoints(server service.TSKVService_WritePointsServer) error {
	for {
		req, err := server.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.accumulator.AddError(fmt.Errorf("failed to receive WritePointsRequest: %w", err))
			return server.Send(&service.WritePointsResponse{
				PointsNumber: 0,
			})
		}

		var points models.Points
		flatbuffers.GetRootAs(req.Points, 0, &points)

		var table models.Table
		for tblI := 0; tblI < points.TablesLength(); tblI++ {
			if !points.Tables(&table, tblI) {
				continue
			}
			tableName := string(table.Tab())

			rowReader, err := makeRowReader(&table)
			if err != nil {
				s.accumulator.AddError(err)
			}

			for {
				fields, tags, ts, hasNext := rowReader.Next()
				s.accumulator.AddFields(tableName, fields, tags, *ts)
				if !hasNext {
					break
				}
			}
		}

	}

	return nil
}

func makeRowReader(table *models.Table) (*models.RowReader, error) {
	var tagReaders []*models.StringReader
	var fieldReaders []interface{}
	var timeReader *models.IntReader

	numRows := int(table.NumRows())
	var column models.Column
	for colI := 0; colI < table.ColumnsLength(); colI++ {
		if !table.Columns(&column, colI) {
			continue
		}

		switch column.ColumnType() {
		case models.ColumnTypeTime:
			if r, err := models.NewIntReader(&column, numRows); err == nil {
				timeReader = r
			} else {
				return nil, fmt.Errorf("failed to parse time column from WritePointsRequest: %w", err)
			}
		case models.ColumnTypeTag:
			if r, err := models.NewStringReader(&column, numRows); err == nil {
				tagReaders = append(tagReaders, r)
			} else {
				return nil, fmt.Errorf("failed to parse tag column from WritePointsRequest: %w", err)
			}
		case models.ColumnTypeField:
			switch column.FieldType() {
			case models.FieldTypeFloat:
				if r, err := models.NewFloatReader(&column, numRows); err == nil {
					fieldReaders = append(fieldReaders, r)
				} else {
					return nil, fmt.Errorf("failed to parse float column from WritePointsRequest: %w", err)
				}
			case models.FieldTypeInteger:
				if r, err := models.NewIntReader(&column, numRows); err == nil {
					fieldReaders = append(fieldReaders, r)
				} else {
					return nil, fmt.Errorf("failed to parse integer column from WritePointsRequest: %w", err)
				}
			case models.FieldTypeUnsigned:
			case models.FieldTypeBoolean:
			case models.FieldTypeString:
				if r, err := models.NewStringReader(&column, numRows); err == nil {
					fieldReaders = append(fieldReaders, r)
				} else {
					return nil, fmt.Errorf("failed to parse string column from WritePointsRequest: %w", err)
				}
			default:
				return nil, fmt.Errorf("unknown field type %d from WritePointsRequest", column.FieldType())
			}
		default:
			return nil, fmt.Errorf("unknown column type %d from WritePointsRequest", column.ColumnType())
		}
	}

	return models.NewRowReader(numRows, fieldReaders, tagReaders, timeReader), nil
}
