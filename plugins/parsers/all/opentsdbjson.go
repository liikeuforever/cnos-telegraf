//go:build !custom || parsers || parsers.opentsdbtelnet

package all

import _ "github.com/influxdata/telegraf/plugins/parsers/opentsdbjson" // register plugin
