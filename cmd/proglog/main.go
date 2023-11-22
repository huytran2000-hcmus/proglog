package main

import (
	"log"

	"github.com/spf13/cobra"
)

func main() {
	cli := &cli{}

	cmd := &cobra.Command{
		Use:     "prodlog",
		PreRunE: cli.setupConfig,
		RunE:    cli.run,
	}

	err := setupFlags(cmd)
	if err != nil {
		log.Fatalf("setup flags failed: %s", err)
	}

	err = cmd.Execute()
	if err != nil {
		log.Fatal(err)
	}
}
