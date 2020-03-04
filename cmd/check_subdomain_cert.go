package cmd

import (
	"fmt"
	"os"

	"github.com/Huuancao/sentinel/pkg/config"
	"github.com/spf13/cobra"
)

var (
	domains []string
)

var checkSubdomainCertCmd = &cobra.Command{
	Use:   "checkSubdomainCert",
	Short: "Check the validity of all certificates of all sub-domains of a given domain.",
	Long: `Check the validity of all certificates of all sub-domains of a given domain.

You may provide multiple domains.`,
	Run: func(cmd *cobra.Command, args []string) {
		checkSubdomainCert()
	},
}

func init() {
	RootCmd.AddCommand(checkSubdomainCertCmd)
	//Flags
	checkSubdomainCertCmd.Flags().StringSliceVarP(&domains, "domains", "", []string{}, "Domains to scan for certificates")
}

func checkSubdomainCert() {
	logger, err := config.GetLogger(verbose)
	if err != nil {
		fmt.Printf("Cannot get logger: %s\n", err)
		os.Exit(1)
	}

	if len(domains) == 0 {
		fmt.Println("You have to provide at least one domain!")
		os.Exit(1)
	}
	logger.Debugf("Provided domains: %v", domains)
}
