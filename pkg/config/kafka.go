package config

import (
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
)

func getBrokers() []string {
	brokerList := viper.GetStringSlice("kafka.brokers")
	if len(brokerList) == 0 {
		fmt.Println("You have to provide --brokers as a comma seperated array")
		os.Exit(1)
	}
	//fmt.Printf("Brokers: %v\n", brokerList)
	return brokerList
}

func getKafkaVersion() (*sarama.KafkaVersion, error) {
	valueFile := viper.GetString("kafka.version")
	if valueFile != "" {
		parsedVersion, err := sarama.ParseKafkaVersion(valueFile)
		if err != nil {
			return nil, fmt.Errorf("Error when parsing Kafka version config value: %s", err)
		}
		return &parsedVersion, nil
	}
	// If version is not provided then return the default v1
	return &sarama.V1_0_0_0, nil
}

// returns a Sarama configuration
func getConfig() (*sarama.Config, error) {
	conf := sarama.NewConfig()
	version, err := getKafkaVersion()
	if err != nil {
		return nil, fmt.Errorf("Could not set the Kafka version: %s", err)
	}
	conf.Version = *version
	conf.Consumer.Return.Errors = true
	conf.Admin.Timeout = 30 * time.Second

	return conf, nil
}

// creates a new Sarama Cluster Admin
func newClusterAdmin(brokersList *[]string, conf *sarama.Config) (sarama.ClusterAdmin, error) {
	clusterAdmin, err := sarama.NewClusterAdmin(*brokersList, conf)
	if err != nil {
		return nil, err
	}
	return clusterAdmin, nil
}

// returns a Sarama Cluster Admin
func GetClusterAdmin() (*sarama.ClusterAdmin, error) {
	conf, err := getConfig()
	if err != nil {
		return nil, fmt.Errorf("error in config creation: $s", err)
	}
	brokersList := getBrokers()
	clusterAdmin, err := newClusterAdmin(&brokersList, conf)
	if err != nil {
		return nil, err
	}
	return &clusterAdmin, err
}

// returns a new Kafka client
func GetKafkaClient() (sarama.Client, error) {
	conf, err := getConfig()
	brokerList := getBrokers()
	client, err := sarama.NewClient(brokerList, conf)
	return client, err
}
