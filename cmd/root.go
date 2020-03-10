package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
	verbose bool
)

// Base commande without sub-comandes
var RootCmd = &cobra.Command{
	Use:   "sentinel",
	Short: "sentinel - Your monitoring tool to retrieve certificates from sub-domains or sub-networks and check their validity.",
	Long: `sentinel - Your monitoring tool to check certificates validity.

sentinel allows you to retrieve all the certificates you manage (or forgot about)
by scanning all the sub-domains given a domain or by scanning a sub-network.`,
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	RootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output")

	//RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file")
}

// initConfig checks in config file and/or ENV variables if set
func initConfig() {
	viper.SetConfigName("sentinel")
	// Later we might add some configuration file to store the results in a DB
	// viper.AddConfigPath("/etc/")
	// if the config file is passed explicitly, use this instead of the default one
	/*
		if cfgFile != "" {
			viper.SetConfigFile(cfgFile)
		}
		viper.AutomaticEnv()


		if err := viper.ReadInConfig(); err != nil {
			fmt.Println("Cannot read config:", err)
			os.Exit(1)
		}
	*/
}
