package cnosdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs/cnosdb/protos/models"
	"github.com/influxdata/telegraf/plugins/inputs/cnosdb/protos/service"
)

//go:generate flatc -o internal/models --go --go-namespace models --gen-onefile ./protos/models/models.fbs
//go:generate protoc --go_out=./internal --go-grpc_out=./protos ./protos/service/kv_service.proto

var _ service.TSKVServiceServer = (*TSKVServiceServerImpl)(nil)

var errTelegrafClosed = errors.New("telegraf closed")

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

		//db := string(points.DbBytes())
		var table models.Table
		var schema models.Schema
		var point models.Point
		var tag models.Tag
		var field models.Field
		for tblI := 0; tblI < points.TablesLength(); tblI++ {
			if !points.Tables(&table, tblI) {
				continue
			}
			table.Schema(&schema)
			tab := string(table.TabBytes())
			for ptI := 0; ptI < table.PointsLength(); ptI++ {
				if !table.Points(&point, ptI) {
					continue
				}

				tags := make(map[string]string)
				fields := make(map[string]interface{})

				tagsBitSet := BitSet{
					buf: point.TagsNullbitBytes(),
					len: schema.TagNameLength(),
				}
				if point.TagsLength() > 0 {
					for tagI := 0; tagI < point.TagsLength(); tagI++ {
						if !tagsBitSet.get(tagI) {
							continue
						}
						if !point.Tags(&tag, tagI) {
							continue
						}
						tags[string(schema.TagName(tagI))] = string(tag.ValueBytes())
					}
				}
				fieldsBitSet := BitSet{
					buf: point.FieldsNullbitBytes(),
					len: schema.FieldNameLength(),
				}
				if point.FieldsLength() > 0 {
					for fieldI := 0; fieldI < point.FieldsLength(); fieldI++ {
						if !fieldsBitSet.get(fieldI) {
							continue
						}
						if !point.Fields(&field, fieldI) {
							continue
						}
						switch schema.FieldType(fieldI) {
						case models.FieldTypeInteger:
							v := binary.BigEndian.Uint64(field.ValueBytes())
							fields[string(schema.FieldName(fieldI))] = int64(v)
						case models.FieldTypeUnsigned:
							v := binary.BigEndian.Uint64(field.ValueBytes())
							fields[string(schema.FieldName(fieldI))] = v
						case models.FieldTypeFloat:
							tmp := binary.BigEndian.Uint64(field.ValueBytes())
							v := math.Float64frombits(tmp)
							fields[string(schema.FieldName(fieldI))] = v
						case models.FieldTypeBoolean:
							v := field.ValueBytes()
							fields[string(schema.FieldName(fieldI))] = v[0] == byte(1)
						case models.FieldTypeString:
							v := string(field.ValueBytes())
							fields[string(schema.FieldName(fieldI))] = v
						default:
							// Do nothing ?
						}
					}
				}
				s.accumulator.AddFields(tab, fields, tags)
			}

		}

	}

	return nil
}
