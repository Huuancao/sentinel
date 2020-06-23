package cmd

import (
	"fmt"
	"os"
	"sort"

	"github.com/Huuancao/sentinel/pkg/config"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	silent          bool
	topicsFlag      []string
	groupsFlag      []string
	monitoredGroups []string
	monitoredTopics []string
)

var kafkaStatus = &cobra.Command{
	Use:   "kafkaStatus",
	Short: "Show the status of the Kafka Cluster",
	Long: `Show relevant informations of the Kafka Cluster:
	 - Topics
	 - Partitions
	 - Leader/Replicas
	 - ISR
	 - Consumers and respective offsets per topic and partitions
	 - ...
	`,
	Run: func(cmd *cobra.Command, args []string) {
		status()
	},
}

func init() {
	RootCmd.AddCommand(kafkaStatus)

	kafkaStatus.Flags().StringSliceVarP(&groupsFlag, "groups", "", []string{}, "Groups to be checked")
	kafkaStatus.Flags().StringSliceVarP(&topicsFlag, "topics", "", []string{}, "Topics to be checked")
	kafkaStatus.Flags().BoolVarP(&silent, "silent", "", false, "Do not display Kafka __consumer_offsets")
}

func status() {
	logger, err := config.GetLogger(true)
	if err != nil {
		fmt.Printf("Could not create logger: %s\n", err)
		os.Exit(1)
	}
	if len(groupsFlag) != 0 {
		monitoredGroups = groupsFlag
	} else {
		monitoredGroups = viper.GetStringSlice("kafka.consumerlag.consumergroups")
	}
	if len(topicsFlag) != 0 {
		monitoredTopics = topicsFlag
	} else {
		monitoredTopics = viper.GetStringSlice("kafka.consumerlag.topics")
	}

	if len(monitoredGroups) == 0 {
		logger.Fatal("No configured monitored topics.")
		os.Exit(1)
	}
	if len(monitoredTopics) == 0 {
		logger.Fatal("No configured monitored topics.")
		os.Exit(1)
	}

	// Sorting this in prevision to order the displayed info
	sort.Slice(monitoredGroups, func(i int, j int) bool {
		return monitoredGroups[i] < monitoredGroups[j]
	})

	sort.Slice(monitoredTopics, func(i int, j int) bool {
		return monitoredTopics[i] < monitoredTopics[j]
	})

	client, err := config.GetKafkaClient()
	if err != nil {
		logger.Fatalf("cannot connect to the Kafka cluster: %s\n", err)
		os.Exit(1)
	}
	defer client.Close()

	ca, err := config.GetClusterAdmin()
	if err != nil {
		logger.Fatalf("Could not create a cluster admin: %s\n", err)
		os.Exit(1)
	}

	statusTable := tablewriter.NewWriter(os.Stdout)
	statusTable.SetAlignment(tablewriter.ALIGN_LEFT)
	statusTable.SetHeader([]string{"Topic", "Partition", "Leader", "Replicas", "ISR"})
	consumerOffsetTable := tablewriter.NewWriter(os.Stdout)
	consumerOffsetTable.SetAlignment(tablewriter.ALIGN_LEFT)
	consumerOffsetTable.SetHeader([]string{"Consumer Group", "Topic", "Partition", "Consumer Offset"})

	// Retrieving the partitions information (ID, Leader, Replicas, ISR)
	for _, topic := range monitoredTopics {
		if silent {
			if topic == "__consumer_offsets" {
				break
			}
			client.RefreshMetadata(topic)
			partitions, err := client.Partitions(topic)

			if err != nil {
				logger.Errorf("cannot get partitions for topic %s: %s", topic, err)
			}

			sort.Slice(partitions, func(i int, j int) bool {
				return partitions[i] < partitions[j]
			})

			for _, part := range partitions {
				isr, _ := client.InSyncReplicas(topic, part)
				rep, _ := client.Replicas(topic, part)
				lead, _ := client.Leader(topic, part)
				statusTable.Append([]string{topic, fmt.Sprintf("%d", part), fmt.Sprintf("%d", lead.ID()), fmt.Sprintf("%d", rep), fmt.Sprintf("%d", isr)})
			}

		}

	}

	// Retrieving the consumer offsets per topic
	for _, group := range monitoredGroups {
		for _, topic := range monitoredTopics {
			consumerOffsetsPerTopicPartitions, err := config.GetConsumerGroupOffsets(group, client, ca, false)
			if err != nil {
				//logger.Debugf("%s\n", err)
				break
			}
			kafkaTopics := []string{}
			for t, _ := range consumerOffsetsPerTopicPartitions {
				kafkaTopics = append(kafkaTopics, t)
			}

			if config.StringInArray(topic, kafkaTopics) {
				partitions := []int32{}
				for k, _ := range consumerOffsetsPerTopicPartitions[topic] {
					partitions = append(partitions, k)
				}
				sort.Slice(partitions, func(i int, j int) bool {
					return partitions[i] < partitions[j]
				})

				for _, k := range partitions {
					consumerOffsetTable.Append([]string{group, topic, fmt.Sprintf("%d", k), fmt.Sprintf("%d", consumerOffsetsPerTopicPartitions[topic][k])})
				}
			}
		}
	}

	statusTable.Render()
	consumerOffsetTable.Render()
}
