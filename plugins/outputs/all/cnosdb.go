//go:build !custom || outputs || outputs.cnosdb

package all

import _ "github.com/influxdata/telegraf/plugins/outputs/cnosdb" // register plugin
