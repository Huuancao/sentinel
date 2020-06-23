package cmd

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"

	"github.com/Huuancao/sentinel/pkg/config"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
)

const defaultMessageLimit = 1000

var (
	topic      string
	partitions string
	limit      int
	partition  int32
	offset     int64
	initOffset int64
	newest     bool
	oldest     bool
)

var bufferSize = flag.Int("buffer-size", 256, "The buffer size of the message channel.")

var monitorTopicCmd = &cobra.Command{
	Use:   "kafkaMonitorTopic",
	Short: "Monitor a specific Kafka topic.",
	Long: `Monitor a specific Kafka topic.

Consume and monitor a given Kafka topic given arbitrary offsets.`,
	Run: func(cmd *cobra.Command, args []string) {
		monitorTopic()
	},
}

func init() {
	RootCmd.AddCommand(monitorTopicCmd)

	monitorTopicCmd.Flags().IntVarP(&limit, "limit", "", defaultMessageLimit, "Limits maximum amount of displayed messages")
	monitorTopicCmd.Flags().StringVarP(&topic, "topic", "", "", "Display messages of given topic")
	// newest is by default
	monitorTopicCmd.Flags().BoolVarP(&newest, "newest", "", false, "Display messages from the newest offset")
	monitorTopicCmd.Flags().BoolVarP(&oldest, "oldest", "", false, "Display messages from the oldest offset")
	monitorTopicCmd.Flags().Int64VarP(&offset, "offset", "", 0, "Display messages from given offset of a given partition")
	monitorTopicCmd.Flags().StringVarP(&partitions, "partitions", "", "all", "Partitions to be consumed (all or comma-separated numbers)")
}

// returns the list of available partitions
func getPartitions(c sarama.Consumer) ([]int32, error) {
	if partitions == "all" {
		return c.Partitions(topic)
	}
	parts := strings.Split(partitions, ",")
	var partList []int32
	for i := range parts {
		val, err := strconv.ParseInt(parts[i], 10, 32)
		if err != nil {
			return nil, err
		}
		partList = append(partList, int32(val))
	}
	return partList, nil
}

// creates a consumer to consume and display message of a given topic
func monitorTopic() {
	logger, err := config.GetLogger(true)
	if err != nil {
		fmt.Printf("Could not create logger: %s\n", err)
		os.Exit(1)
	}
	//logger.Debugf("Provided config: topic: %s, limit: %d, newest: %t, oldest: %t, offset: %d, partitions: %v,", topic, limit, newest, oldest, offset, partitions)

	client, err := config.GetKafkaClient()
	if err != nil {
		logger.Fatalf("cannot connect to the Kafka cluster: %s\n", err)
		os.Exit(1)
	}
	defer client.Close()

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		logger.Fatalf("cannot create a Kafka consumer: %s\n", err)
		os.Exit(1)
	}
	defer consumer.Close()

	partitionList, err := getPartitions(consumer)
	if err != nil {
		logger.Fatalf(" cannot retrieve the list of partitions: %s\n", err)
		os.Exit(1)
	}

	if oldest {
		initOffset = sarama.OffsetOldest
	} else if offset != 0 {
		initOffset = offset
	} else {
		initOffset = sarama.OffsetNewest
	}

	//logger.Debugf("Offset: %v", initOffset)

	var (
		messageChan = make(chan *sarama.ConsumerMessage, *bufferSize)
		closingChan = make(chan struct{})
		wg          sync.WaitGroup
	)

	// go routine to handle ending signals -> closing channel
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		logger.Info("Initiating Shutdown of the consumer...")
		os.Exit(1)
		closingChan <- struct{}{}
	}()

	// Reading the messages and forwarding it to the messageChan
	for _, partition := range partitionList {
		partConsumer, err := consumer.ConsumePartition(topic, partition, initOffset)
		if err != nil {
			logger.Fatalf("cannot create a Sarama partition consumer given topic %s and partition %d with offset %d: %s\n", topic, partition, initOffset, err)
			os.Exit(1)
		}
		wg.Add(1)
		go func(partConsumer sarama.PartitionConsumer) {
			for {
				select {
				case <-closingChan:
					wg.Done()
					partConsumer.Close()
					logger.Info("Closing Sarama partition consumer")
					return
				case message := <-partConsumer.Messages():
					messageChan <- message
				}
			}
		}(partConsumer)
	}

	// Displaying the messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		msgCount := 0
		for {
			select {
			case <-closingChan:
				wg.Done()
				return
			case message := <-messageChan:
				if msgCount >= limit {
					closingChan <- struct{}{}
				}
				fmt.Printf("Partition: %d\t Offset: %d\tMessage: %s\n", message.Partition, message.Offset, string(message.Value))
				msgCount++
			}
		}
	}()
	wg.Wait()
	fmt.Printf("Done consuming topic %s\n", topic)

	close(messageChan)
	close(closingChan)
}
