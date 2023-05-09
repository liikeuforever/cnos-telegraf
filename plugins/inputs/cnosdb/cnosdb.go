//go:generate ../../../tools/readme_config_includer/generator
package cnosdb

import (
	_ "embed"
	"fmt"
	"net"
	"sync"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/cnosdb/protos/service"
	"google.golang.org/grpc"
)

//go:embed sample.conf
var sampleConfig string

type CnosDB struct {
	ServiceAddress string          `toml:"service_address"`
	Timeout        config.Duration `toml:"timeout"`

	Log telegraf.Logger `toml:"-"`

	wg sync.WaitGroup `toml:"-"`

	listener   net.Listener `toml:"-"`
	grpcServer *grpc.Server `toml:"-"`
}

func init() {
	inputs.Add("cnosdb", func() telegraf.Input {
		return &CnosDB{
			ServiceAddress: ":8803",
		}
	})
}

func (*CnosDB) SampleConfig() string {
	return sampleConfig
}

func (c *CnosDB) Init() error {
	c.Log.Info("Initialization completed.")
	return nil
}

func (c *CnosDB) Gather(_ telegraf.Accumulator) error {
	return nil
}

func (c *CnosDB) Start(acc telegraf.Accumulator) error {
	c.grpcServer = grpc.NewServer(grpc.MaxRecvMsgSize(10 * 1024 * 1024))
	service.RegisterTSKVServiceServer(c.grpcServer, NewTSKVService(acc))

	if c.listener == nil {
		listener, err := net.Listen("tcp", c.ServiceAddress)
		if err != nil {
			return err
		}
		c.listener = listener
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		if err := c.grpcServer.Serve(c.listener); err != nil {
			acc.AddError(fmt.Errorf("failed to stop CnosDB gRPC service: %w", err))
		}
	}()

	c.Log.Infof("Listening on %s", c.listener.Addr().String())

	return nil
}

func (c *CnosDB) Stop() {
	if c.grpcServer != nil {
		c.grpcServer.Stop()
	}
	c.wg.Wait()
}
