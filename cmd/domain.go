package cmd

import (
    "fmt"
    "os"

    "github.com/spf13/cobra"
)

var (
    domains     []string
)


var domainCmd = &cobra.Command{
    Use:   "domain",
    Short: "Check the validity of all certificates of all sub-domains of a given domain.",
    Long: `Check the validity of all certificates of all sub-domains of a given domain.

You may provide multiple domains.`,
    Run: func(cmd *cobra.Command, args []string) {
        domainCheck()
    },
}

func init() {
    RootCmd.AddCommand(domainCmd)

    domainCmd.Flags().StringSliceVarP(&domains, "domains", "", []string{}, "Domains to scan for certificates")
}

func domainCheck() {
    if len(domains) == 0 {
        fmt.Println("You have to provide at least one domain!")
        os.Exit(1)
    }
}
