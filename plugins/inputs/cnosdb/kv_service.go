package cnosdb

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	service "github.com/influxdata/telegraf/plugins/inputs/cnosdb/protos/service"
)

//go:generate flatc -o internal/models --go --go-namespace models --gen-onefile ./protos/models/models.fbs
//go:generate protoc --go_out=./protos/service --go-grpc_out=./protos/service ./protos/service/kv_service.proto

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

func (s TSKVServiceServerImpl) WriteSubscription(server service.TSKVService_WriteSubscriptionServer) error {
	for {
		req, err := server.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("failed to receive WritePointsRequest: %v", err)
			s.accumulator.AddError(fmt.Errorf("failed to receive WritePointsRequest: %w", err))
			return server.Send(&service.SubscriptionResponse{})
		}

		var tableschema TskvTableSchema
		if err := json.Unmarshal(req.TableSchema, &tableschema); err != nil {
			log.Printf("Error unmarshal: %v", err)
			return server.Send(&service.SubscriptionResponse{})
		}

		recordReader, err := ipc.NewReader(bufio.NewReader(bytes.NewReader(req.RecordData)))
		if err != nil {
			log.Printf("failed to read record data: %v", err)
			s.accumulator.AddError(fmt.Errorf("failed to read record data: %w", err))
			return server.Send(&service.SubscriptionResponse{})
		}

		tab := tableschema.Name

		columnIndexToType := make([]string, len(tableschema.Columns))
		columnIndexToName := make([]string, len(tableschema.Columns))

		for _, col := range tableschema.Columns {
			columnIndexToType[col.ID] = col.ColumnType
			columnIndexToName[col.ID] = col.Name
		}
		for {
			r, err := recordReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("failed to read record data: %v", err)
				s.accumulator.AddError(fmt.Errorf("failed to read record data: %w", err))
				continue
			}

			numRows := r.NumRows()
			numCols := r.NumCols()
			colArrMap := make([]arrow.Array, len(tableschema.Columns))

			for j, col := range r.Columns() {
				switch columnIndexToType[j] {
				case "TAG_STRING":
					colArrMap[j] = array.NewStringData(col.Data())
				case "FIELD_STRING":
					colArrMap[j] = array.NewStringData(col.Data())
				case "FIELD_BIGINT":
					colArrMap[j] = array.NewInt64Data(col.Data())
				case "FIELD_BIGINT UNSIGNED":
					colArrMap[j] = array.NewUint64Data(col.Data())
				case "FIELD_DOUBLE":
					colArrMap[j] = array.NewFloat64Data(col.Data())
				case "FIELD_BOOLEAN":
					colArrMap[j] = array.NewBooleanData(col.Data())
				case "TIME_TIMESTAMP(SECOND)", "TIME_TIMESTAMP(MILLISECOND)", "TIME_TIMESTAMP(MICROSECOND)", "TIME_TIMESTAMP(NANOSECOND)":
					colArrMap[j] = array.NewTime64Data(col.Data())
				}
			}

			for i := 0; i < int(numRows); i++ {
				tags := make(map[string]string)
				fields := make(map[string]interface{})
				times := time.Unix(0, 0)
				hasTimestamp := false
				for j := 0; j < int(numCols); j++ {
					if colArrMap[j].IsNull(i) {
						continue
					}
					switch columnIndexToType[uint64(j)] {
					case "TAG_STRING":
						tags[columnIndexToName[j]] = colArrMap[j].(*array.String).Value(i)
					case "FIELD_STRING":
						fields[columnIndexToName[j]] = colArrMap[j].(*array.String).Value(i)
					case "FIELD_BIGINT":
						fields[columnIndexToName[j]] = colArrMap[j].(*array.Int64).Value(i)
					case "FIELD_BIGINT UNSIGNED":
						fields[columnIndexToName[j]] = colArrMap[j].(*array.Uint64).Value(i)
					case "FIELD_DOUBLE":
						fields[columnIndexToName[j]] = colArrMap[j].(*array.Float64).Value(i)
					case "FIELD_BOOLEAN":
						fields[columnIndexToName[j]] = colArrMap[j].(*array.Boolean).Value(i)
					case "TIME_TIMESTAMP(SECOND)":
						hasTimestamp = true
						times = time.Unix(int64(colArrMap[j].(*array.Time64).Value(i)), 0)
					case "TIME_TIMESTAMP(MILLISECOND)":
						hasTimestamp = true
						times = time.UnixMilli(int64(colArrMap[j].(*array.Time64).Value(i)))
					case "TIME_TIMESTAMP(MICROSECOND)":
						hasTimestamp = true
						times = time.UnixMicro(int64(colArrMap[j].(*array.Time64).Value(i)))
					case "TIME_TIMESTAMP(NANOSECOND)":
						hasTimestamp = true
						times = time.Unix(0, int64(colArrMap[j].(*array.Time64).Value(i)))
					}
				}
				if hasTimestamp {
					s.accumulator.AddMetric(metric.New(tab, tags, fields, times))
				} else {
					s.accumulator.AddFields(tab, fields, tags)
				}
			}
		}
	}
	return nil
}
