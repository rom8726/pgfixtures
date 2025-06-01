package pgfixtures

import (
	"fmt"

	"github.com/rom8726/pgfixtures/internal/db"
)

type Config struct {
	FilePath     string
	ConnStr      string
	DatabaseType db.DatabaseType
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
		c.DatabaseType = db.PostgreSQL
	}

	return nil
}
