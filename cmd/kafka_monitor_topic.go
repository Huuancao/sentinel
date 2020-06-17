package cmd

import (
	"flag"
	"fmt"
	"os"

	"github.com/Huuancao/sentinel/pkg/config"
	"github.com/spf13/cobra"
)

const defaultMessageLimit = 1000

var (
	topic      string
	partitions string
	limit      int
	offset     int64
	newest     bool
	oldest     bool
)

var bufferSize = flag.Int("buffer-size", 256, "The buffer size of the message channel.")

var monitorCmd = &cobra.Command{
	Use:   "kafkaMonitor",
	Short: "Monitor a specific Kafka topic.",
	Long: `Monitor a specific Kafka topic.

Consume and monitor a given Kafka topic given arbitrary offsets.`,
	Run: func(cmd *cobra.Command, args []string) {
		monitorTopic()
	},
}

func init() {
	RootCmd.AddCommand(monitorCmd)

	monitorCmd.Flags().IntVarP(&limit, "limit", "", defaultMessageLimit, "Limits maximum amount of displayed messages")
	monitorCmd.Flags().StringVarP(&topic, "topic", "", "", "Display messages of given topic")
	// newest is by default
	monitorCmd.Flags().BoolVarP(&newest, "newest", "", false, "Display messages from the newest offset")
	monitorCmd.Flags().BoolVarP(&oldest, "oldest", "", false, "Display messages from the oldest offset")
	monitorCmd.Flags().Int64VarP(&offset, "offset", "", 0, "Display messages from given offset of a given partition")
	monitorCmd.Flags().StringVarP(&partitions, "partitions", "", "all", "Partitions to be consumed (all or comma-separated numbers)")
	// yeah whatever, will parse that shit later... Cannot put default string "all" in an IntSliceVarP...
}

// getPartitions returns the list of available partitions

// monitorTopic
func monitorTopic() {
	logger, err := config.GetLogger(true)
	if err != nil {
		fmt.Printf("Could not create logger: %s\n", err)
		os.Exit(1)
	}
	logger.Debugf("Provided config: topic: %s, limit: %d, newest: %t, oldest: %t, offset: %d, partitions: %v,", topic, limit, newest, oldest, offset, partitions)
}
