package cmd

import (
	"fmt"
	"os"

	"github.com/Huuancao/sentinel/pkg/config"
	"github.com/spf13/cobra"
)

var (
	subnets []string
)

var checkSubnetCertCmd = &cobra.Command{
	Use:   "checkSubnetCert",
	Short: "Check the validity of all certificates in a given subnet.",
	Long: `Check the validity of all certificates in a given subnet.

You may provide multiple subnets.`,
	Run: func(cmd *cobra.Command, args []string) {
		checkSubnetCert()
	},
}

func init() {
	RootCmd.AddCommand(checkSubnetCertCmd)
	//Flags
	checkSubnetCertCmd.Flags().StringSliceVarP(&subnets, "subnets", "", []string{}, "Subnets to scan for certificates")
}

func checkSubnetCert() {
	logger, err := config.GetLogger(verbose)
	if err != nil {
		fmt.Printf("Cannot get logger: %s\n", err)
		os.Exit(1)
	}

	if len(subnets) == 0 {
		fmt.Println("You have to provide at least one subnet!")
		os.Exit(1)
	}
	logger.Debugf("Provided subnets: %v", subnets)
}
