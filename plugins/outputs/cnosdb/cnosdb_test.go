package cnosdb

import (
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
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
	PrintPoints(data)
}
