package pgfixtures

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rom8726/pgfixtures/internal/loader"
)

func Load(ctx context.Context, config *Config) error {
	if err := config.Validate(); err != nil {
		return fmt.Errorf("validate config: %w", err)
	}

	db, err := sql.Open("postgres", config.ConnStr)
	if err != nil {
		return fmt.Errorf("connect to DB: %w", err)
	}
	defer db.Close()

	l := loader.Loader{
		DB: db,
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
