package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "pgfixtures",
	Short: "PostgreSQL fixtures loader",
}

func Execute() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		panic(err)
	}
}
