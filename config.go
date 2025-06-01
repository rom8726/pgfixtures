package pgfixtures

import (
	"fmt"
)

// DatabaseType represents the type of database
type DatabaseType string

const (
	// PostgreSQL database type
	PostgreSQL DatabaseType = "postgres"
	// MySQL database type
	MySQL DatabaseType = "mysql"
)

type Config struct {
	FilePath     string
	ConnStr      string
	DatabaseType DatabaseType
	Truncate     bool
	ResetSeq     bool
	DryRun       bool
}

func (c *Config) Validate() error {
	if c.FilePath == "" {
		return fmt.Errorf("file path is required")
	}
	if c.ConnStr == "" {
		return fmt.Errorf("connection string is required")
	}
	if c.DatabaseType == "" {
		// Default to PostgreSQL for backward compatibility
		c.DatabaseType = PostgreSQL
	}

	return nil
}
