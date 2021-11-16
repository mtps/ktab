package cmd

import (
	"fmt"
	"github.com/mtps/ktab/pkg/versioninfo"
	"github.com/spf13/cobra"
	"os"
)

var cmdRoot = &cobra.Command{
	Use:     "ktab",
	Version: versioninfo.Current().String(),
}

func init() {
	// cmdRoot.AddCommand()
}

func Execute() {
	if err := cmdRoot.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
