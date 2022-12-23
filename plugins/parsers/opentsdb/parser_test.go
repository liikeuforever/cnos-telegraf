package opentsdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const JsonArray = `[
	{
		"metric": "sys.cpu.nice",
		"timestamp": 1346846400,
		"value": 18,
		"tags": {
			"host": "web01",
			"dc": "lga"
		}
	},
	{
		"metric": "sys.cpu.nice",
		"timestamp": 1346846400,
		"value": 9,
		"tags": {
			"host": "web02",
			"dc": "lga"
		}
	}
]`

const JsonObject = `{
	"metric": "sys.cpu.nice",
	"timestamp": 1346846400,
	"value": 18,
	"tags": {
		"host": "web01",
		"dc": "lga"
	}
}`

func TestParseJSONArray(t *testing.T) {
	parser := &Parser{}
	metrics, err := parser.Parse([]byte(JsonArray))
	require.NoError(t, err)
	require.Len(t, metrics, 2)
	require.Equal(t, "sys.cpu.nice", metrics[0].Name())
	require.Equal(t, map[string]string{
		"host": "web01",
		"dc":   "lga",
	}, metrics[0].Tags())
	require.Equal(t, map[string]interface{}{
		"value": float64(18),
	}, metrics[0].Fields())
	require.Equal(t, time.Unix(1346846400, 0), metrics[0].Time())
}

func TestParseJSONObject(t *testing.T) {
	parser := &Parser{}
	metrics, err := parser.Parse([]byte(JsonObject))
	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Equal(t, "sys.cpu.nice", metrics[0].Name())
	require.Equal(t, map[string]string{
		"host": "web01",
		"dc":   "lga",
	}, metrics[0].Tags())
	require.Equal(t, map[string]interface{}{
		"value": float64(18),
	}, metrics[0].Fields())
	require.Equal(t, time.Unix(1346846400, 0), metrics[0].Time())
}
