package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
	_ "github.com/lib/pq"              // PostgreSQL driver
	"github.com/spf13/cobra"

	"github.com/rom8726/pgfixtures"
	"github.com/rom8726/pgfixtures/internal/loader"
)

var (
	file     string
	connStr  string
	dbType   string
	truncate bool
	resetSeq bool
	dryRun   bool
)

func init() {
	cmd := &cobra.Command{
		Use:   "load",
		Short: "Load fixtures into a database",
		RunE:  func(cmd *cobra.Command, args []string) error { return runLoad(cmd.Context()) },
	}

	cmd.Flags().StringVarP(&file, "file", "f", "fixtures.yml", "Path to YAML fixture file")
	cmd.Flags().StringVar(&connStr, "db", "", "Database connection string (required)")
	cmd.Flags().StringVar(&dbType, "db-type", "postgres", "Database type (postgres or mysql)")
	cmd.Flags().BoolVar(&truncate, "truncate", true, "Truncate tables before loading")
	cmd.Flags().BoolVar(&resetSeq, "reset-seq", true, "Reset sequences after loading")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Print actions without executing")

	_ = cmd.MarkFlagRequired("db")
	rootCmd.AddCommand(cmd)
}

func runLoad(ctx context.Context) error {
	// Convert string database type to DatabaseType
	var databaseType pgfixtures.DatabaseType
	switch strings.ToLower(dbType) {
	case "postgres", "postgresql":
		databaseType = pgfixtures.PostgreSQL
	case "mysql":
		databaseType = pgfixtures.MySQL
	default:
		return fmt.Errorf("unsupported database type: %s (supported types: postgres, mysql)", dbType)
	}

	// Create database implementation
	database, err := pgfixtures.NewDatabase(databaseType)
	if err != nil {
		return fmt.Errorf("create database implementation: %w", err)
	}

	// Get the appropriate database driver name
	var driverName string
	switch databaseType {
	case pgfixtures.PostgreSQL:
		driverName = "postgres"
	case pgfixtures.MySQL:
		driverName = "mysql"
	}

	// Open database connection
	sqlDB, err := sql.Open(driverName, connStr)
	if err != nil {
		return fmt.Errorf("connect to DB: %w", err)
	}
	defer sqlDB.Close()

	// Create and run loader
	l := loader.Loader{
		DB:       sqlDB,
		Database: database,
		Config: loader.LoaderConfig{
			FilePath: file,
			Truncate: truncate,
			ResetSeq: resetSeq,
			DryRun:   dryRun,
		},
	}

	if err := l.Load(ctx); err != nil {
		return fmt.Errorf("load fixtures: %w", err)
	}

	log.Println("Fixtures loaded successfully")

	return nil
}
