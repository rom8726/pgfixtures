package loader

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rom8726/pgfixtures/internal/db"
	"github.com/rom8726/pgfixtures/internal/parser"
)

type LoaderConfig struct {
	FilePath string
	Truncate bool
	ResetSeq bool
	DryRun   bool
}

type Loader struct {
	DB       *sql.DB
	Config   LoaderConfig
	Database db.Database
}

func (l *Loader) Load(ctx context.Context) error {
	fixtures, err := parser.ParseFile(l.Config.FilePath)
	if err != nil {
		return err
	}

	tables := make([]string, 0, len(fixtures))
	for t := range fixtures {
		tables = append(tables, t)
	}

	deps, err := l.Database.GetDependencyGraph(ctx, l.DB)
	if err != nil {
		return err
	}

	sorted, err := db.TopoSort(deps, tables)
	if err != nil {
		return err
	}

	tx, err := l.DB.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	if l.Config.Truncate {
		if err := l.truncateTables(ctx, tx, sorted); err != nil {
			_ = tx.Rollback()

			return err
		}
	}

	for i := len(sorted) - 1; i >= 0; i-- {
		table := sorted[i]

		records := fixtures[table]
		for _, row := range records {
			if err := l.insertRow(ctx, tx, table, row); err != nil {
				_ = tx.Rollback()

				return fmt.Errorf("insert into %q: %w", table, err)
			}
		}
	}

	if l.Config.ResetSeq {
		if err := l.resetSequences(ctx, tx, sorted); err != nil {
			_ = tx.Rollback()

			return err
		}
	}

	return tx.Commit()
}

func (l *Loader) truncateTables(ctx context.Context, tx *sql.Tx, tables []string) error {
	return l.Database.TruncateTables(ctx, tx, tables, l.Config.DryRun)
}

func (l *Loader) insertRow(ctx context.Context, tx *sql.Tx, table string, row map[string]any) error {
	// Process $eval expressions
	processedRow := make(map[string]any)
	for col, val := range row {
		if expr, ok := parser.IsEval(val); ok {
			if err := tx.QueryRowContext(ctx, expr).Scan(&val); err != nil {
				return fmt.Errorf("eval %q: %w", expr, err)
			}
		}
		processedRow[col] = val
	}

	return l.Database.InsertRow(ctx, tx, table, processedRow, l.Config.DryRun)
}

func (l *Loader) resetSequences(ctx context.Context, tx *sql.Tx, tables []string) error {
	return l.Database.ResetSequences(ctx, tx, tables, l.Config.DryRun)
}
