package cmd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Huuancao/sentinel/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	metricsSocket = "/var/run/prometheus/kafka_consumer_lag"
)

var (
	metricConsumerOffset = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_lag",
			Help: "Current consumer lag for a consumer group, topic and partition",
		},
		[]string{
			"topic",
			"partition",
			"group",
		},
	)
)

var monitorConsumerLagCmd = &cobra.Command{
	Use:   "kafkaConsumerlag",
	Short: "Monitor the consumer lag of a specific kafka topic(s).",
	Long:  `Monitor the consumer lag of a specific kafka topic(s).`,
	Run: func(cmd *cobra.Command, args []string) {
		monitorConsumerLag()
	},
}

var (
	maxDuration int
	minDuration int
	refresh     int
	groups      []string
	topics      []string
)

func init() {
	RootCmd.AddCommand(monitorConsumerLagCmd)

	prometheus.MustRegister(metricConsumerOffset)
}

func monitorConsumerLag() {
	logger, err := config.GetLogger(true)
	if err != nil {
		fmt.Printf("Could not create logger: %s\n", err)
		os.Exit(1)
	}

	minDuration = viper.GetInt("kafka.consumerlag.minduration")
	maxDuration = viper.GetInt("kafka.consumerlag.maxduration")
	refresh = viper.GetInt("kafka.consumerlag.refresh")
	monitoredGroups = viper.GetStringSlice("kafka.consumerlag.consumergroups")
	monitoredTopics = viper.GetStringSlice("kafka.consumerlag.topics")

	scrapeConfig, err := config.NewScrapeConfig(refresh, minDuration, maxDuration, monitoredGroups, monitoredTopics)
	if err != nil {
		logger.Fatalf("cannot create Scrape config: %s\n", err)
		os.Exit(1)
	}

	client, err := config.GetKafkaClient()
	if err != nil {
		logger.Fatalf("cannot connect to Kafka: %s\n", err)
		os.Exit(1)
	}
	defer client.Close()

	ca, err := config.GetClusterAdmin()
	if err != nil {
		logger.Fatalf("Could not create cluster admin: %s\n", err)
		os.Exit(1)
	}

	ctx := context.Background()

	enforceGracefulShutdown(func(wg *sync.WaitGroup, shutdownChan chan struct{}, errorChan chan error) {
		config.StartKafkaScraper(wg, shutdownChan, errorChan, client, ca, metricConsumerOffset, scrapeConfig)
		startPrometheus(wg, shutdownChan, ctx)
	})

}

// starts the Prometheus server to expose the metrics
func startPrometheus(wg *sync.WaitGroup, shutdownChan chan struct{}, c context.Context) {
	logger, err := config.GetLogger(true)
	if err != nil {
		fmt.Printf("Could not create logger: %s\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(c)
	defer cancel()

	srv := &http.Server{
		Addr:    "/metrics",
		Handler: promhttp.Handler(),
	}

	wg.Add(1)
	defer wg.Done()

	listener, errListen := net.Listen("unix", metricsSocket)
	if errListen != nil {
		logger.Errorf("Failed to initialize metrics socket: %s", errListen.Error())
		os.Exit(1)
	}
	go func() {
		httpErr := srv.Serve(listener)
		if httpErr != nil {
			logger.Errorf("Prometheus server shutting down: %s", httpErr.Error())
		}
	}()

	<-shutdownChan
	logger.Info("Shutting down metrics server...")
	srv.Shutdown(ctx)
	listener.Close()
	os.Remove(metricsSocket)
}

// monitors and propagates shutdown signals
func enforceGracefulShutdown(f func(wg *sync.WaitGroup, shutdownChan chan struct{}, errorChan chan error)) {
	logger, err := config.GetLogger(true)
	if err != nil {
		fmt.Printf("Could not create logger: %s\n", err)
		os.Exit(1)
	}
	wg := &sync.WaitGroup{}
	shutdownChan := make(chan struct{})
	signalChan := make(chan os.Signal)
	errorChan := make(chan error, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		select {
		case err := <-errorChan:
			logger.Infof("Shutting down, %s", err)
			close(shutdownChan)
		case <-signalChan:
			close(shutdownChan)
		}
	}()

	f(wg, shutdownChan, errorChan)

	<-shutdownChan
	logger.Info("Initiating shutdown of the consumer lag monitoring...")
	wg.Wait()
}
