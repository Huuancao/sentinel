package config

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
)

type ScrapeConfig struct {
	FetchMinInterval        int
	FetchMaxInterval        int
	MetadataRefreshInterval int
	Topics                  []string
	Groups                  []string
}

func NewScrapeConfig(refresh int, fetchMin int, fetchMax int, groups []string, topics []string) (ScrapeConfig, error) {
	if len(topics) == 0 {
		return ScrapeConfig{}, errors.New("no monitored topic provided")
	}
	if len(groups) == 0 {
		return ScrapeConfig{}, errors.New("no monitored consumer groups provided\n")
	}

	// set default values
	if fetchMin < 10 || fetchMin > 30 {
		fetchMin = 10
	}

	if fetchMax < 30 || fetchMax > 60 {
		fetchMax = 30
	}

	if refresh < 0 || refresh > 60 {
		refresh = 30
	}

	return ScrapeConfig{
		FetchMinInterval:        fetchMin,
		FetchMaxInterval:        fetchMax,
		MetadataRefreshInterval: refresh,
		Groups:                  groups,
		Topics:                  topics,
	}, nil
}

func getBrokers() ([]string, error) {
	brokerList := viper.GetStringSlice("kafka.brokers")
	if len(brokerList) == 0 {
		return brokerList, errors.New("You have to provide --brokers as a comma seperated array")
	}
	//fmt.Printf("Brokers: %v\n", brokerList)
	return brokerList, nil
}

func getKafkaVersion() (*sarama.KafkaVersion, error) {
	valueFile := viper.GetString("kafka.version")
	if valueFile != "" {
		parsedVersion, err := sarama.ParseKafkaVersion(valueFile)
		if err != nil {
			return nil, errors.Wrap(err, "Error when parsing Kafka version config value")
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
		return nil, errors.Wrap(err, "Could not set the Kafka version")
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
func StringInArray(a string, array []string) bool {
	for _, b := range array {
		if a == b {
			return true
		}
	}

	return false
}

// returns the elements that are in both provided arrayss
func getArrayIntersection(firstArray []string, secondArray []string) []string {
	intersection := []string{}

	for _, i := range firstArray {
		if StringInArray(i, secondArray) {
			intersection = append(intersection, i)
		}
	}

	return intersection
}

// returns a Sarama Cluster Admin
func GetClusterAdmin() (sarama.ClusterAdmin, error) {
	conf, err := getConfig()
	if err != nil {
		return nil, err
	}
	brokersList, err := getBrokers()
	if err != nil {
		return nil, err
	}
	clusterAdmin, err := newClusterAdmin(&brokersList, conf)
	if err != nil {
		return nil, err
	}

	return clusterAdmin, err
}

// returns a new Kafka client
func GetKafkaClient() (sarama.Client, error) {
	conf, err := getConfig()
	brokerList, err := getBrokers()
	if err != nil {
		return nil, err
	}
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

// returns the configured topics in the Kafka cluster
func GetTopics(ca sarama.ClusterAdmin) ([]string, error) {
	topics := []string{}
	kafkaTopics, err := ca.ListTopics()
	if err != nil {
		return topics, err
	}
	for topic, _ := range kafkaTopics {
		topics = append(topics, topic)
	}
	sort.Strings(topics)

	return topics, nil
}

// returns the offsets or consumer lag for given consumer group for all topics
func GetConsumerGroupOffsets(group string, client sarama.Client, ca sarama.ClusterAdmin, lag bool) (map[string]map[int32]int64, error) {
	consumerOffsetsPerTopicPartitions := map[string]map[int32]int64{}
	offsetFetchResponse, err := ca.ListConsumerGroupOffsets(group, nil)
	if err != nil {
		e := errors.Errorf("failed to retrieve the offsets for group %s", group)
		return nil, e
	}

	for topic, blocks := range offsetFetchResponse.Blocks {
		consumerOffsetsPerTopicPartitions[topic] = map[int32]int64{}
		for partition, block := range blocks {
			newestOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				e := errors.Errorf("failed to retrieve the newest offsets for topic: %s, partition %d", topic, partition)
				return nil, e
			}
			// might as well already handle the consumer lag
			if lag {
				consumerOffsetsPerTopicPartitions[topic][partition] = newestOffset - block.Offset
			} else {
				consumerOffsetsPerTopicPartitions[topic][partition] = newestOffset
			}
		}
	}
	return consumerOffsetsPerTopicPartitions, nil
}

// stars Kafka Scraper
func StartKafkaScraper(wg *sync.WaitGroup, shutdownChan chan struct{}, errorChan chan error, client sarama.Client, ca sarama.ClusterAdmin, consumerOffset *prometheus.GaugeVec, scrapeConfig ScrapeConfig) {
	go refreshMetadata(wg, shutdownChan, errorChan, client, scrapeConfig)
	go manageConsumerLag(wg, shutdownChan, errorChan, client, ca, scrapeConfig, consumerOffset)
}

// refreshes the metadata
func refreshMetadata(wg *sync.WaitGroup, shutdownChan chan struct{}, errorChan chan error, client sarama.Client, scrapeConfig ScrapeConfig) {
	logger, err := GetLogger(true)
	if err != nil {
		fmt.Printf("Could not create logger: %s\n", err)
		errorChan <- err
	}

	wg.Add(1)
	defer wg.Done()
	wait := time.After(0)
	for {
		select {
		case <-wait:
			err := client.RefreshMetadata()
			if err != nil {
				logger.Errorf("Failed to refresh the cluster metadata: %s", err)
			}
		case <-shutdownChan:
			logger.Printf("Initiating refreshMetadata shutdown of the consumer lag broker handler...")
			return
		}

		// I forgot that crap was in nanosec...
		delay := time.Duration(scrapeConfig.MetadataRefreshInterval) * 1000000000
		wait = time.After(delay)
	}
}

// queries and manages the consumer lag data
func manageConsumerLag(wg *sync.WaitGroup, shutdownChan chan struct{}, errorChan chan error, client sarama.Client, ca sarama.ClusterAdmin, scrapeConfig ScrapeConfig, metricOffsetConsumer *prometheus.GaugeVec) {
	logger, err := GetLogger(true)
	if err != nil {
		e := errors.Wrap(err, "could not create logger")
		errorChan <- e
	}

	topicsKafka, err := GetTopics(ca)
	if err != nil {
		e := errors.Wrap(err, "failed to retrieve topics from the cluster")
		errorChan <- e
	}
	groupsKafka, err := GetConsumerGroups(ca)
	if err != nil {
		e := errors.Wrap(err, "failed to retrieve consumer groups from the cluster")
		errorChan <- e
	}

	// retrieve the monitored topics configured in the Kafka cluster
	topics := getArrayIntersection(scrapeConfig.Topics, topicsKafka)
	groups := getArrayIntersection(scrapeConfig.Groups, groupsKafka)

	numberRequests := len(groups)

	wg.Add(1)
	defer wg.Done()
	wait := time.After(0)
	for {
		select {
		case <-wait:
			requestWG := &sync.WaitGroup{}
			requestWG.Add(2 + numberRequests)
			for _, group := range groups {
				go func(group string, topics []string, client sarama.Client, ca sarama.ClusterAdmin, metricOffsetConsumer *prometheus.GaugeVec) {
					requestWG.Done()
					consumerOffsets, err := GetConsumerGroupOffsets(group, client, ca, true)
					for _, topic := range topics {
						for partition, _ := range consumerOffsets[topic] {
							metricOffsetConsumer.With(prometheus.Labels{
								"topic":     topic,
								"partition": strconv.Itoa(int(partition)),
								"group":     group,
							}).Set(float64(consumerOffsets[topic][partition]))
						}
					}
					if err != nil {
						errorChan <- err
					}
				}(group, topics, client, ca, metricOffsetConsumer)
			}

		case <-shutdownChan:
			logger.Printf("Initiating shutdown of the consumer lag broker handler...")
			return
		}

		min := int64(scrapeConfig.FetchMinInterval)
		max := int64(scrapeConfig.FetchMaxInterval)
		// this crap is AGAIN in nanosec...
		duration := time.Duration((rand.Int63n(max - min)) * 1000000000)

		wait = time.After(duration)
	}
}
