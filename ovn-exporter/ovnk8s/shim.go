package ovnk8s

import (
	"sync"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	"github.com/rs/zerolog/log"
	kexec "k8s.io/utils/exec"
	libovsdbclient "github.com/ovn-kubernetes/libovsdb/client"
)

type Register interface {
	SetExec() error
	RegisterOvsMetricsWithOvnMetrics(ovsDBClient libovsdbclient.Client, metricsScrapeInterval int, stopChan <-chan struct{})
	RegisterOvnDBMetrics(stopChan <-chan struct{})
	RegisterOvnControllerMetrics(ovsDBClient libovsdbclient.Client, metricsScrapeInterval int, stopChan <-chan struct{})
	RegisterOvnNorthdMetrics(stopChan <-chan struct{})
	StartOVNMetricsServer(bindAddress, certFile, keyFile string, stopChan <-chan struct{}, wg *sync.WaitGroup)
}

type OvnK8sShim interface {
	Register
}

type shim struct{}

func NewOvnK8sShim() OvnK8sShim {
	return &shim{}
}

func (s *shim) SetExec() error {
	if err := util.SetExec(kexec.New()); err != nil {
		log.Error().Err(err).Msg("SetExec error")
		return err
	}
	return nil
}

func (s *shim) RegisterOvnDBMetrics(stopChan <-chan struct{}) {
	metrics.RegisterOvnDBMetrics(
		func() bool { return true },
		stopChan,
	)
}

func (s *shim) RegisterOvnControllerMetrics(ovsDBClient libovsdbclient.Client, metricsScrapeInterval int, stopChan <-chan struct{}) {
	metrics.RegisterOvnControllerMetrics(ovsDBClient, metricsScrapeInterval, stopChan)
}

func (s *shim) RegisterOvnNorthdMetrics(stopChan <-chan struct{}) {
	metrics.RegisterOvnNorthdMetrics(
		func() bool { return true },
		stopChan,
	)
}

func (s *shim) RegisterOvsMetricsWithOvnMetrics(ovsDBClient libovsdbclient.Client, metricsScrapeInterval int, stopChan <-chan struct{}) {
	metrics.RegisterOvsMetricsWithOvnMetrics(ovsDBClient, metricsScrapeInterval, stopChan)
}

func (s *shim) StartOVNMetricsServer(bindAddress, certFile, keyFile string, stopChan <-chan struct{}, wg *sync.WaitGroup) {
	metrics.StartOVNMetricsServer(bindAddress, certFile, keyFile, stopChan, wg)
}
