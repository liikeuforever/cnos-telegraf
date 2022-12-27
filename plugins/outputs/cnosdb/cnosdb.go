package cnosdb

import (
	"context"
	_ "embed"
	"encoding/binary"
	"math"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/outputs/cnosdb/internal/models"
	"github.com/influxdata/telegraf/plugins/outputs/cnosdb/internal/service"
)

//go:embed sample.conf
var sampleConfig string

type CnosdbWriter struct {
	Url      string `toml:"url"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Database string `toml:"database"`

	Client service.TSKVService_WritePointsClient `toml:"-"`
	Log    telegraf.Logger                       `toml:"-"`
}

func init() {
	outputs.Add("cnosdb", func() telegraf.Output {
		return &CnosdbWriter{
			Url:      "localhost:31006",
			User:     "user",
			Password: "password",
			Database: "pts",
		}
	})
}

func (*CnosdbWriter) SampleConfig() string {
	return sampleConfig
}

func (cw *CnosdbWriter) Init() error {
	cw.Log.Info("Initialization completed.")
	return nil
}

func (cw *CnosdbWriter) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cw.Log.Infof("Connecting to CnosDB: '%s'", cw.Url)
	conn, err := grpc.DialContext(ctx, cw.Url, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		cw.Log.Errorf("Error connecting to CnosDB: '%s'", err)
		return err
	}
	cw.Log.Infof("Connected to CnosDB: '%s'", cw.Url)

	grpcCli := service.NewTSKVServiceClient(conn)
	writePointsCli, err := grpcCli.WritePoints(context.Background())
	if err != nil {
		cw.Log.Errorf("Error creating gRPC client: '%s'", err)
		return err
	}
	cw.Client = writePointsCli

	return nil
}

func (cw *CnosdbWriter) Close() error {
	return cw.Client.CloseSend()
}

func (cw *CnosdbWriter) Write(metrics []telegraf.Metric) error {
	data := cw.parsePoints(metrics)
	err := cw.Client.Send(&service.WritePointsRpcRequest{
		Points: data,
	})
	if err != nil {
		cw.Log.Errorf("Error writing points: '%s'", err)
		return err
	}
	return nil
}

func (cw *CnosdbWriter) parsePoints(metrics []telegraf.Metric) []byte {
	fb := flatbuffers.NewBuilder(0)
	database := fb.CreateByteString([]byte(cw.Database))
	numLines := len(metrics)

	pointOffs := make([]flatbuffers.UOffsetT, len(metrics))
	for i, metric := range metrics {
		pointOffs[i] = cw.parsePoint(fb, metric)
	}

	models.PointsStartPointsVector(fb, numLines)
	for _, i := range pointOffs {
		fb.PrependUOffsetT(i)
	}
	pointsVecOff := fb.EndVector(numLines)
	models.PointsStart(fb)
	models.PointsAddPoints(fb, pointsVecOff)
	models.PointsAddDb(fb, database)
	pointsModelOff := models.PointsEnd(fb)
	fb.Finish(pointsModelOff)

	return fb.FinishedBytes()
}

func (cw *CnosdbWriter) parsePoint(fb *flatbuffers.Builder, metric telegraf.Metric) flatbuffers.UOffsetT {
	dbOff := fb.CreateByteVector([]byte(cw.Database))
	tableOff := fb.CreateByteVector([]byte(metric.Name()))

	tagsVecOff := cw.putMetricTags(fb, metric)
	fieldsVecOff := cw.putMetricFields(fb, metric)

	models.PointStart(fb)
	models.PointAddDb(fb, dbOff)
	models.PointAddTab(fb, tableOff)
	models.PointAddTags(fb, tagsVecOff)
	models.PointAddFields(fb, fieldsVecOff)
	models.PointAddTimestamp(fb, metric.Time().UnixNano())

	return models.PointEnd(fb)
}

func (cw *CnosdbWriter) putMetricTags(fb *flatbuffers.Builder, metric telegraf.Metric) flatbuffers.UOffsetT {
	metricTags := metric.TagList()
	tkOffs := make([]flatbuffers.UOffsetT, len(metricTags))
	tvOffs := make([]flatbuffers.UOffsetT, len(metricTags))
	for i, tag := range metricTags {
		tkOffs[i] = fb.CreateByteVector([]byte(tag.Key))
		tvOffs[i] = fb.CreateByteVector([]byte(tag.Value))
	}

	tagOffs := make([]flatbuffers.UOffsetT, len(tkOffs))
	for i := 0; i < len(tkOffs); i++ {
		models.TagStart(fb)
		models.TagAddKey(fb, tkOffs[i])
		models.TagAddValue(fb, tvOffs[i])
		tagOffs[i] = models.TagEnd(fb)
	}
	models.PointStartTagsVector(fb, len(tagOffs))
	for _, off := range tagOffs {
		fb.PrependUOffsetT(off)
	}

	return fb.EndVector(len(tagOffs))
}

func (cw *CnosdbWriter) putMetricFields(fb *flatbuffers.Builder, metric telegraf.Metric) flatbuffers.UOffsetT {
	metricFields := metric.FieldList()
	fkOffs := make([]flatbuffers.UOffsetT, len(metricFields))
	fvOffs := make([]flatbuffers.UOffsetT, len(metricFields))
	fTypes := make([]models.FieldType, len(metricFields))
	numBuf := make([]byte, 8)
	for i, field := range metricFields {
		fkOffs[i] = fb.CreateByteString([]byte(field.Key))
		switch field.Value.(type) {
		case int64:
			binary.BigEndian.PutUint64(numBuf, uint64(field.Value.(int64)))
			fvOffs[i] = fb.CreateByteVector(numBuf)
			fTypes[i] = models.FieldTypeInteger
		case uint64:
			binary.BigEndian.PutUint64(numBuf, field.Value.(uint64))
			fvOffs[i] = fb.CreateByteVector(numBuf)
			fTypes[i] = models.FieldTypeUnsigned
		case float64:
			binary.BigEndian.PutUint64(numBuf, math.Float64bits(field.Value.(float64)))
			fvOffs[i] = fb.CreateByteVector(numBuf)
			fTypes[i] = models.FieldTypeFloat
		case bool:
			if field.Value.(bool) {
				fvOffs[i] = fb.CreateByteVector([]byte{1})
			} else {
				fvOffs[i] = fb.CreateByteVector([]byte{0})
			}
			fTypes[i] = models.FieldTypeBoolean
		case string:
			fv := field.Value.(string)
			if fv[0] == '"' {
				fvOffs[i] = fb.CreateByteVector([]byte(fv[1 : len(fv)-1]))
			} else {
				fvOffs[i] = fb.CreateByteVector([]byte(fv))
			}
			fTypes[i] = models.FieldTypeString
		default:
			cw.Log.Errorf("Invalid value type: %s: %T(%v)\n", field.Key, field.Value, field.Value)
		}
	}

	fieldOffs := make([]flatbuffers.UOffsetT, len(fkOffs))
	for i := 0; i < len(fkOffs); i++ {
		models.FieldStart(fb)
		models.FieldAddName(fb, fkOffs[i])
		models.FieldAddValue(fb, fvOffs[i])
		models.FieldAddType(fb, fTypes[i])
		fieldOffs[i] = models.FieldEnd(fb)
	}
	models.PointStartFieldsVector(fb, len(fieldOffs))
	for _, off := range fieldOffs {
		fb.PrependUOffsetT(off)
	}

	return fb.EndVector(len(fieldOffs))
}
