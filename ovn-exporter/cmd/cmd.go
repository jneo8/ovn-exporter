package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/canonical/ovn-exporter/ovn-exporter/config"
	"github.com/canonical/ovn-exporter/ovn-exporter/ovnk8s"
	"github.com/ovn-kubernetes/libovsdb/client"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/vswitchd"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	"github.com/spf13/cobra"
)

var cfg config.Config

var rootCmd = &cobra.Command{
	Use:               config.AppName,
	RunE:              run,
	Short:             config.ShortDesc,
	PersistentPreRunE: persistentPreRun,
}

func init() {
	rootCmd.Flags().String("loglevel", "debug", "log level")
	rootCmd.Flags().String("host", "0.0.0.0", "prometheus server host")
	rootCmd.Flags().String("port", "9310", "prometheus server port")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func persistentPreRun(cmd *cobra.Command, args []string) error {
	viper.AutomaticEnv()
	viper.SetEnvPrefix(config.EnvPrefix)
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		return fmt.Errorf("unable to bind flags: %w", err)
	}
	if err := viper.Unmarshal(&cfg); err != nil {
		return fmt.Errorf("unable to decode config")
	}
	log.Debug().Msgf("config %#v", cfg)
	return nil
}

func run(cmd *cobra.Command, args []string) error {
	stopChan := make(chan struct{})
	wg := sync.WaitGroup{}

	ovnK8sShim := ovnk8s.NewOvnK8sShim()

	if err := ovnK8sShim.SetExec(); err != nil {
		return err
	}

	// Create OVS DB client
	dbModel, err := vswitchd.FullDatabaseModel()
	if err != nil {
		return fmt.Errorf("failed to get OVS database model: %w", err)
	}

	ovsClient, err := client.NewOVSDBClient(dbModel, client.WithEndpoint("unix:/var/snap/microovn/common/run/switch/db.sock"))
	if err != nil {
		return fmt.Errorf("failed to create OVS client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := ovsClient.Connect(ctx); err != nil {
		log.Warn().Err(err).Msg("Failed to connect to OVS database, metrics may be limited")
		return err
	}

	metricsScrapeInterval := 30 // Default scrape interval in seconds

	ovnK8sShim.RegisterOvsMetricsWithOvnMetrics(ovsClient, metricsScrapeInterval, stopChan)
	ovnK8sShim.RegisterOvnDBMetrics(stopChan)
	ovnK8sShim.RegisterOvnControllerMetrics(ovsClient, metricsScrapeInterval, stopChan)
	ovnK8sShim.RegisterOvnNorthdMetrics(stopChan)

	ovnK8sShim.StartOVNMetricsServer(
		cfg.BindAddress(), "", "", stopChan, &wg,
	)
	wg.Wait()
	close(stopChan)
	return nil
}
