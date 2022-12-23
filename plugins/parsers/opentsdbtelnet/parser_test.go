package opentsdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	parser := &Parser{}
	metrics, err := parser.Parse([]byte(`put sys.cpu.user 1356998400 42.5 host=webserver01 cpu=0
put sys.cpu.user 1356998400 50.5 host=webserver01 cpu=2`))
	require.NoError(t, err)
	require.Len(t, metrics, 2)

	require.Equal(t, "sys.cpu.user", metrics[0].Name())
	require.Equal(t, map[string]string{
		"host": "webserver01",
		"cpu":  "0",
	}, metrics[0].Tags())
	require.Equal(t, map[string]interface{}{
		"value": float64(42.5),
	}, metrics[0].Fields())
	require.Equal(t, time.Unix(1356998400, 0), metrics[0].Time())

	require.Equal(t, "sys.cpu.user", metrics[1].Name())
	require.Equal(t, map[string]string{
		"host": "webserver01",
		"cpu":  "2",
	}, metrics[1].Tags())
	require.Equal(t, map[string]interface{}{
		"value": float64(50.5),
	}, metrics[1].Fields())
	require.Equal(t, time.Unix(1356998400, 0), metrics[1].Time())
}
