package cmd

import (
    "fmt"
    "os"

    "github.com/spf13/cobra"
)

var (
    subnets     []string
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

    checkSubnetCertCmd.Flags().StringSliceVarP(&subnets, "subnets", "", []string{}, "Subnets to scan for certificates")
}

func checkSubnetCert() {
    if len(subnets) == 0 {
        fmt.Println("You have to provide at least one subnet!")
        os.Exit(1)
    }
}
