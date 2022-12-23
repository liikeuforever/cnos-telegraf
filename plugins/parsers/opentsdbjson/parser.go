package opentsdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/parsers"
)

func init() {
	parsers.Add("opentsdb",
		func(_ string) telegraf.Parser {
			return &Parser{}
		},
	)
}

type point struct {
	Metric string            `json:"metric"`
	Time   int64             `json:"timestamp"`
	Value  float64           `json:"value"`
	Tags   map[string]string `json:"tags,omitempty"`
}

type Parser struct {
	DefaultTags map[string]string `toml:"-"`
}

func (p *Parser) Parse(buf []byte) ([]telegraf.Metric, error) {
	var multi bool
	switch buf[0] {
	case '{':
	case '[':
		multi = true
	default:
		return nil, errors.New("expected JSON array or hash")
	}

	points := make([]point, 1)
	if dec := json.NewDecoder(bytes.NewReader(buf)); multi {
		if err := dec.Decode(&points); err != nil {
			return nil, errors.New("json array decode error for data format: opentsdb")
		}
	} else {
		if err := dec.Decode(&points[0]); err != nil {
			return nil, errors.New("json object decode error for data format: opentsdb")
		}
	}

	metrics := make([]telegraf.Metric, 0, len(points))
	for i := range points {
		pt := points[i]

		// Convert timestamp to Go time.
		// If time value is over a billion then it's microseconds.
		var ts time.Time
		if pt.Time < 10000000000 {
			ts = time.Unix(pt.Time, 0)
		} else {
			ts = time.Unix(pt.Time/1000, (pt.Time%1000)*1000)
		}

		var tags map[string]string
		if len(p.DefaultTags) > 0 {
			tags = make(map[string]string)
			for k, v := range p.DefaultTags {
				tags[k] = v
			}
			for k, v := range pt.Tags {
				tags[k] = v
			}
		} else {
			tags = pt.Tags
		}

		mt := metric.New(pt.Metric, tags, map[string]interface{}{"value": pt.Value}, ts)
		metrics = append(metrics, mt)
	}

	return metrics, nil
}

func (p *Parser) ParseLine(line string) (telegraf.Metric, error) {
	metrics, err := p.Parse([]byte(line))
	if err != nil {
		return nil, err
	}

	if len(metrics) < 1 {
		return nil, fmt.Errorf("can not parse the line: %s, for data format: opentsdb", line)
	}

	return metrics[0], nil
}

func (p *Parser) SetDefaultTags(tags map[string]string) {
	p.DefaultTags = tags
}
