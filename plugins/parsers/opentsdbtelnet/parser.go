package opentsdb

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/parsers"
)

func init() {
	parsers.Add("opentsdbtelnet",
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
	r := bufio.NewReader(bytes.NewReader(buf))
	metrics := make([]telegraf.Metric, 0, 1)
	for {
		line, _, err := r.ReadLine()
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			break
		}

		parts := strings.Fields(string(line))
		if len(parts) < 4 || parts[0] != "put" {
			continue
		}
		measurement := parts[1]
		tsStr := parts[2]
		valueStr := parts[3]
		tagStrs := parts[4:]

		var t time.Time
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("malformed time: %s", tsStr)
		}
		switch len(tsStr) {
		case 10:
			t = time.Unix(ts, 0)
		case 13:
			t = time.Unix(ts/1000, (ts%1000)*1000)
		default:
			//continue
			return nil, fmt.Errorf("time must be 10 or 13 chars: %s", tsStr)
		}

		tags := make(map[string]string)
		for t := range tagStrs {
			parts := strings.SplitN(tagStrs[t], "=", 2)
			if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
				return nil, fmt.Errorf("malformed tag data: %s", tagStrs[t])
			}
			k := parts[0]
			tags[k] = parts[1]
		}

		fields := make(map[string]interface{})
		fv, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			//continue
			return nil, fmt.Errorf("bad float: %s", valueStr)
		}
		fields["value"] = fv

		mt := metric.New(measurement, tags, fields, t)
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
		return nil, fmt.Errorf("can not parse the line: %s, for data format: opentsdbtelnet", line)
	}

	return metrics[0], nil
}

func (p *Parser) SetDefaultTags(tags map[string]string) {
	p.DefaultTags = tags
}
