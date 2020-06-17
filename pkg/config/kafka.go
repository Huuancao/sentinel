package config

import (
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
)

type ClusterAdmin struct {
	Client sarama.ClusterAdmin
}

func GetBrokers() []string {
	brokerList := viper.GetStringSlice("kafka.brokers")
	if len(brokerList) == 0 {
		fmt.Println("You have to provide --brokers as an array")
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
	// If version is not provided then return the default v1.
	return &sarama.V1_0_0_0, nil
}

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

func getClusterAdmin() (*ClusterAdmin, error) {
	conf, err := getConfig()
	if err != nil {
		return nil, fmt.Errorf("error in config creation: $s", err)
	}
	brokersList := GetBrokers()
	clusterAdmin, err := NewClusterAdmin(&brokersList, conf)
	if err != nil {
		return nil, err
	}
	return clusterAdmin, err
}

// getKafkaClient returns a new client
func GetKafkaClient() (sarama.Client, error) {
	conf, err := getConfig()
	brokerList := GetBrokers()
	// Create new client
	client, err := sarama.NewClient(brokerList, conf)
	return client, err
}

func NewClusterAdmin(brokersList *[]string, conf *sarama.Config) (*ClusterAdmin, error) {
	c := &ClusterAdmin{}
	client, err := sarama.NewClusterAdmin(*brokersList, conf)
	if err != nil {
		return nil, err
	}
	c.Client = client
	return c, nil
}
