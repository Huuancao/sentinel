package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
)

var (
	name      = "Sentinel"
	version   = "" // version is set by the compiler.
	gitCommit = "" // gitCommit is set by the compiler.
	buildDate = "" // buildDate is set by the compiler.
	goVersion = "" // goVersion is set by the compiler.
	platform  = "" // platform is set by the compiler.
)

type versionInfo struct {
	Name      string `json:"name,omitempty"`
	Version   string `json:"version,omitempty"`
	Commit    string `json:"commit,omitempty"`
	BuildDate string `json:"build_date,omitempty"`
	GoVersion string `json:"go_version,omitempty"`
	Platform  string `json:"platform,omitempty"`
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version information.",
	Long:  "Print the version information.",

	Run: func(cmd *cobra.Command, args []string) {
		printVersion()
	},
}

func init() {
	RootCmd.AddCommand(versionCmd)
}

func printVersion() {
	version, _ := json.Marshal(&versionInfo{
		Name:      name,
		Version:   version,
		Commit:    gitCommit,
		BuildDate: buildDate,
		GoVersion: goVersion,
		Platform:  platform,
	})

	fmt.Println(string(version))
}
