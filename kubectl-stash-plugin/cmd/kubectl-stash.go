package main

import (
	"os"

	"github.com/spf13/pflag"
	"k8s.io/cli-runtime/pkg/genericclioptions"

	"github.com/njhale/cmstore/kubectl-stash-plugin/pkg/cmd"
)

func main() {
	flags := pflag.NewFlagSet("kubectl-stash", pflag.ExitOnError)
	pflag.CommandLine = flags

	root := cmd.NewCmdStash(genericclioptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr})
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
