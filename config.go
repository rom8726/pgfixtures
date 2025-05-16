package pgfixtures

import (
	"fmt"
)

type Config struct {
	FilePath string
	ConnStr  string
	Truncate bool
	ResetSeq bool
	DryRun   bool
}

func (c *Config) Validate() error {
	if c.FilePath == "" {
		return fmt.Errorf("file path is required")
	}
	if c.ConnStr == "" {
		return fmt.Errorf("connection string is required")
	}

	return nil
}
