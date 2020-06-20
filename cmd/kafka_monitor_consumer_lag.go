package cmd

import (
	"fmt"
	"os"

	"github.com/Huuancao/sentinel/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	metricsSocket = "/var/run/prometheus/kafka_consumer_lag"
)

var (
	metricOffsetConsumer = prometheus.NewGaugeVec(
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

	prometheus.MustRegister(metricOffsetConsumer)
}

func monitorConsumerLag() {
	minDuration = viper.GetInt("kafka.consumerlag.minduration")
	maxDuration = viper.GetInt("kafka.consumerlag.maxduration")
	refresh = viper.GetInt("kafka.consumerlag.refresh")
	groups = viper.GetStringSlice("kafka.consumerlag.consumergroups")
	topics = viper.GetStringSlice("kafka.consumerlag.topics")

	logger, err := config.GetLogger(true)
	if err != nil {
		fmt.Printf("Could not create logger: %s\n", err)
		os.Exit(1)
	}
	logger.Debugf("Provided config: min: %d, max: %d, groups: %v, topics: %v,", minDuration, maxDuration, groups, topics)

	//getClusterAdmin

	//getKafkaClient

	//gracefulShutdown

}

//startPrometheus

//gracefulShutdown
