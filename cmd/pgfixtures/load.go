package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"github.com/spf13/cobra"

	"github.com/rom8726/pgfixtures/internal/loader"
)

var (
	file     string
	connStr  string
	truncate bool
	resetSeq bool
	dryRun   bool
)

func init() {
	cmd := &cobra.Command{
		Use:   "load",
		Short: "Load fixtures into PostgreSQL",
		RunE:  func(cmd *cobra.Command, args []string) error { return runLoad(cmd.Context()) },
	}

	cmd.Flags().StringVarP(&file, "file", "f", "fixtures.yml", "Path to YAML fixture file")
	cmd.Flags().StringVar(&connStr, "db", "", "PostgreSQL connection string (required)")
	cmd.Flags().BoolVar(&truncate, "truncate", true, "Truncate tables before loading")
	cmd.Flags().BoolVar(&resetSeq, "reset-seq", true, "Reset sequences after loading")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Print actions without executing")

	_ = cmd.MarkFlagRequired("db")
	rootCmd.AddCommand(cmd)
}

func runLoad(context.Context) error {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("connect to DB: %w", err)
	}
	defer db.Close()

	l := loader.Loader{
		DB: db,
		Config: loader.LoaderConfig{
			FilePath: file,
			Truncate: truncate,
			ResetSeq: resetSeq,
			DryRun:   dryRun,
		},
	}

	if err := l.Load(); err != nil {
		return fmt.Errorf("load fixtures: %w", err)
	}

	log.Println("Fixtures loaded successfully")

	return nil
}
