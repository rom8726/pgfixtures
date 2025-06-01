package pgfixtures

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
	_ "github.com/lib/pq"              // PostgreSQL driver

	"github.com/rom8726/pgfixtures/internal/db"
	"github.com/rom8726/pgfixtures/internal/loader"
)

func Load(ctx context.Context, config *Config) error {
	if err := config.Validate(); err != nil {
		return fmt.Errorf("validate config: %w", err)
	}

	// Get the appropriate database driver name
	var driverName string
	switch config.DatabaseType {
	case db.PostgreSQL:
		driverName = "postgres"
	case db.MySQL:
		driverName = "mysql"
	default:
		return fmt.Errorf("unsupported database type: %s", config.DatabaseType)
	}

	// Open database connection
	database, err := sql.Open(driverName, config.ConnStr)
	if err != nil {
		return fmt.Errorf("connect to DB: %w", err)
	}
	defer database.Close()

	// Create database implementation
	dbImpl, err := db.NewDatabase(config.DatabaseType)
	if err != nil {
		return fmt.Errorf("create database implementation: %w", err)
	}

	l := loader.Loader{
		DB:       database,
		Database: dbImpl,
		Config: loader.LoaderConfig{
			FilePath: config.FilePath,
			Truncate: config.Truncate,
			ResetSeq: config.ResetSeq,
			DryRun:   config.DryRun,
		},
	}

	if err := l.Load(ctx); err != nil {
		return fmt.Errorf("load fixtures: %w", err)
	}

	return nil
}
