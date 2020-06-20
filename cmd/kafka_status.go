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
	silent bool
)

var kafkaStatus = &cobra.Command{
	Use:   "kafkaStatus",
	Short: "Show the status of the Kafka Cluster",
	Long: `Show relevant informations of the Kafka Cluster:
	 - Partitions
	 - Topics
	 - Consumers and respective Offsets
	 - ...
	`,
	Run: func(cmd *cobra.Command, args []string) {
		status()
	},
}

func init() {
	RootCmd.AddCommand(kafkaStatus)

	kafkaStatus.Flags().BoolVarP(&silent, "silent", "", false, "Do not display Kafka __consumer_offsets")
}

func status() {
	logger, err := config.GetLogger(true)
	if err != nil {
		fmt.Printf("Could not create logger: %s\n", err)
		os.Exit(1)
	}
	monitoredGroups := viper.GetStringSlice("kafka.consumerlag.consumergroups")
	monitoredTopics := viper.GetStringSlice("kafka.consumerlag.topics")

	if len(monitoredGroups) == 0 {
		logger.Printf("No configured monitored topics\n")
	}
	if len(monitoredTopics) == 0 {
		logger.Printf("No configured monitored topics\n")
	}

	sort.Slice(monitoredGroups, func(i int, j int) bool {
		return monitoredGroups[i] < monitoredGroups[j]
	})

	sort.Slice(monitoredTopics, func(i int, j int) bool {
		return monitoredTopics[i] < monitoredTopics[j]
	})

	client, err := config.GetKafkaClient()
	if err != nil {
		logger.Printf("cannot connect to Kafka: %s\n", err)
		os.Exit(1)
	}
	defer client.Close()

	ca, err := config.GetClusterAdmin()
	if err != nil {
		logger.Printf("Could not create cluster admin: %s\n", err)
		os.Exit(1)
	}
	scrapeConfig := config.NewScrapeConfig(monitoredTopics, monitoredGroups)
	if err != nil {
		fmt.Printf("Could not create scrape config: %s\n", err)
		os.Exit(1)
	}

	allTopics, err := config.GetTopics(ca, scrapeConfig)
	if err != nil {
		logger.Printf("cannot get all the topics from Kafka: %s", err)
		os.Exit(1)
	}

	statusTable := tablewriter.NewWriter(os.Stdout)
	statusTable.SetAlignment(tablewriter.ALIGN_LEFT)
	statusTable.SetHeader([]string{"Topic", "Partition", "Leader", "Replicas", "ISR"})
	consumerOffsetTable := tablewriter.NewWriter(os.Stdout)
	consumerOffsetTable.SetAlignment(tablewriter.ALIGN_LEFT)
	consumerOffsetTable.SetHeader([]string{"Topic", "Partition", "Consumer Group", "Consumer Offset"})

	for topic, _ := range allTopics {
		if silent {
			if topic == "__consumer_offsets" {
				break
			}
			client.RefreshMetadata(topic)
			partitions, err := client.Partitions(topic)

			if err != nil {
				logger.Warn("cannot get partitions for topic %s: %s", topic, err)
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

	for _, topic := range monitoredTopics {
		for _, group := range monitoredGroups {
			consumerGroupOffsets, err := config.GetConsumerGroupOffsets(group, topic, client, ca, false)
			if err != nil {
				logger.Printf("%s\n", err)
				break
			}
			keys := []int32{}
			for k, _ := range consumerGroupOffsets {
				keys = append(keys, k)
			}
			sort.Slice(keys, func(i int, j int) bool {
				return keys[i] < keys[j]
			})

			for _, k := range keys {
				consumerOffsetTable.Append([]string{topic, fmt.Sprintf("%d", k), fmt.Sprintf("%s", group), fmt.Sprintf("%d", consumerGroupOffsets[k])})
			}
		}
	}

	statusTable.Render()
	consumerOffsetTable.Render()
}
