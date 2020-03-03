package cmd

import (
    "fmt"
    "os"

    "github.com/spf13/cobra"
)

var (
    domains     []string
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

    checkSubdomainCertCmd.Flags().StringSliceVarP(&domains, "domains", "", []string{}, "Domains to scan for certificates")
}

func checkSubdomainCert() {
    if len(domains) == 0 {
        fmt.Println("You have to provide at least one domain!")
        os.Exit(1)
    }
    fmt.Printf("Content: %v", domains)
}
