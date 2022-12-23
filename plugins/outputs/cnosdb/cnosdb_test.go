package cnosdb

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/outputs/cnosdb/internal/models"
)

var now = time.Date(2020, 6, 30, 16, 16, 0, 0, time.UTC)

func TestParsePoints(t *testing.T) {
	cw := &CnosdbWriter{
		Url:      "localhost:31006",
		User:     "user",
		Password: "password",
		Database: "dba",
	}

	ms := []telegraf.Metric{metric.New(
		"cpu",
		map[string]string{"name": "cpu1"},
		map[string]interface{}{"idle": 50, "sys": 30},
		now,
	)}

	data := cw.parsePoints(ms)
	printPoints(data)
}

func printPoints(data []byte) {
	fmt.Printf("\nData Length: %v\n", len(data))
	points := models.GetRootAsPoints(data, 0)
	point := &models.Point{}
	tag := &models.Tag{}
	field := &models.Field{}
	for i := 0; i < points.PointsLength(); i++ {
		points.Points(point, i)
		fmt.Printf("Tags[%d]: ", point.TagsLength())
		for j := 0; j < point.TagsLength(); j++ {
			point.Tags(tag, j)
			if tag.KeyLength() == 0 {
				println("Key is empty")
			}
			tagKey := string(tag.KeyBytes())
			fmt.Printf("{ %s: ", tagKey)
			if tag.KeyLength() == 0 {
				println("Value is empty")
			}
			tagValue := string(tag.ValueBytes())
			fmt.Printf("%s }, ", tagValue)
		}
		fmt.Printf("\nFields[%d]: ", point.FieldsLength())
		for j := 0; j < point.FieldsLength(); j++ {
			point.Fields(field, j)
			fieldName := string(field.NameBytes())
			fmt.Printf("{ %s: ", fieldName)
			fieldType := field.Type()
			switch fieldType {
			case models.FieldTypeInteger:
				fieldValue := binary.BigEndian.Uint64(field.ValueBytes())
				fmt.Printf("%d, ", int64(fieldValue))
			case models.FieldTypeUnsigned:
				fieldValue := binary.BigEndian.Uint64(field.ValueBytes())
				fmt.Printf("%d, ", fieldValue)
			case models.FieldTypeFloat:
				fieldValue := binary.BigEndian.Uint64(field.ValueBytes())
				fmt.Printf("%f, ", float64(fieldValue))
			case models.FieldTypeBoolean:
				fieldValue := field.ValueBytes()
				if fieldValue[0] == 1 {
					fmt.Printf("true, ")
				} else {
					fmt.Printf("false, ")
				}
			case models.FieldTypeString:
				fieldValue := string(field.ValueBytes())
				fmt.Printf("%s, ", fieldValue)
			default:

			}
			fmt.Printf("%d }, ", field.Type())
		}
		fmt.Println()
	}
}
