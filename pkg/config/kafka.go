package config

import (
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type ScrapeConfig struct {
	Topics []string
	Groups []string
}

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

// basic check if string is in the array
func stringInArray(a string, array []string) bool {
	for _, b := range array {
		if a == b {
			return true
		}
	}

	return false
}

// returns a new scrape config
func NewScrapeConfig(topics []string, groups []string) ScrapeConfig {
	return ScrapeConfig{
		Topics: topics,
		Groups: groups,
	}
}

// returns a Sarama Cluster Admin
func GetClusterAdmin() (sarama.ClusterAdmin, error) {
	conf, err := getConfig()
	if err != nil {
		return nil, fmt.Errorf("error in config creation: $s", err)
	}
	brokersList := getBrokers()
	clusterAdmin, err := newClusterAdmin(&brokersList, conf)
	if err != nil {
		return nil, err
	}

	return clusterAdmin, err
}

// returns a new Kafka client
func GetKafkaClient() (sarama.Client, error) {
	conf, err := getConfig()
	brokerList := getBrokers()
	client, err := sarama.NewClient(brokerList, conf)

	return client, err
}

// returns the consumer groups of a Kafka Cluster
func GetConsumerGroups(ca sarama.ClusterAdmin) ([]string, error) {
	groups := []string{}
	clusterGroups, err := ca.ListConsumerGroups()
	if err != nil {
		return groups, err
	}
	for group, _ := range clusterGroups {
		groups = append(groups, group)
	}
	if len(groups) == 0 {
		return groups, errors.Wrap(err, "consumer groups mismatch in config and Kafka")
	}

	return groups, nil
}

// returns the monitored Kafka topics
func GetTopics(ca sarama.ClusterAdmin, cfg ScrapeConfig) (map[string]bool, error) {
	topics := map[string]bool{}
	kafkaTopics, err := ca.ListTopics()
	if err != nil {
		return topics, err
	}
	for kafkaTopic, _ := range kafkaTopics {
		if stringInArray(kafkaTopic, cfg.Topics) {
			topics[kafkaTopic] = true
		} else {
			topics[kafkaTopic] = false
		}
	}

	return topics, nil
}

//returns the offsets or consumer lag for a given topic and given consumer group
func GetConsumerGroupOffsets(group string, topic string, client sarama.Client, ca sarama.ClusterAdmin, lag bool) (map[int32]int64, error) {
	partitionsOffsets := map[int32]int64{}
	offsetFetchResponse, err := ca.ListConsumerGroupOffsets(group, nil)
	if err != nil {
		e := errors.Errorf("failed to retrieve the offsets for group %s", group)
		return nil, e
	}

	for tpc, blocks := range offsetFetchResponse.Blocks {
		if tpc == topic {
			for partition, block := range blocks {
				newestOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					e := errors.Errorf("failed to retrieve the newest offsets for topic: %s, partition %d", topic, partition)
					return nil, e
				}
				// might as well already handle the consumer lag
				if lag {
					partitionsOffsets[partition] = newestOffset - block.Offset
				} else {
					partitionsOffsets[partition] = newestOffset
				}
			}
		}
	}
	return partitionsOffsets, nil
}
