package loader

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

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
	DB     *sql.DB
	Config LoaderConfig
}

func (l *Loader) Load() error {
	fixtures, err := parser.ParseFile(l.Config.FilePath)
	if err != nil {
		return err
	}

	tables := make([]string, 0, len(fixtures))
	for t := range fixtures {
		tables = append(tables, t)
	}

	deps, err := db.GetDependencyGraph(l.DB)
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
		if err := l.truncateTables(tx, sorted); err != nil {
			_ = tx.Rollback()

			return err
		}
	}

	for i := len(sorted) - 1; i >= 0; i-- {
		table := sorted[i]

		records := fixtures[table]
		for _, row := range records {
			if err := l.insertRow(tx, table, row); err != nil {
				_ = tx.Rollback()

				return fmt.Errorf("insert into %q: %w", table, err)
			}
		}
	}

	if l.Config.ResetSeq {
		if err := l.resetSequences(tx, sorted); err != nil {
			_ = tx.Rollback()

			return err
		}
	}

	return tx.Commit()
}

func (l *Loader) truncateTables(tx *sql.Tx, tables []string) error {
	query := "TRUNCATE " + strings.Join(tables, ", ") + " RESTART IDENTITY CASCADE"
	if l.Config.DryRun {
		log.Println("[dry-run]", query)

		return nil
	}

	_, err := tx.Exec(query)

	return err
}

func (l *Loader) insertRow(tx *sql.Tx, table string, row map[string]any) error {
	var cols []string
	var vals []any
	var ph []string

	for col, val := range row {
		if expr, ok := parser.IsEval(val); ok {
			if err := tx.QueryRow(expr).Scan(&val); err != nil {
				return fmt.Errorf("eval %q: %w", expr, err)
			}
		}

		cols = append(cols, col)
		vals = append(vals, val)
		ph = append(ph, fmt.Sprintf("$%d", len(vals)))
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(cols, ", "),
		strings.Join(ph, ", "),
	)

	if l.Config.DryRun {
		log.Printf("[dry-run] %s :: %v", query, vals)

		return nil
	}

	_, err := tx.Exec(query, vals...)

	return err
}

func (l *Loader) resetSequences(tx *sql.Tx, tables []string) error {
	for _, table := range tables {
		query := fmt.Sprintf(`
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT column_default, column_name FROM information_schema.columns
        WHERE table_name = '%s' AND column_default LIKE 'nextval%%'
    LOOP
        EXECUTE format('SELECT setval(pg_get_serial_sequence(''%s'', ''%s''), COALESCE(MAX(%s), 0)) FROM %s',
            r.column_name, r.column_name, r.column_name, '%s');
    END LOOP;
END$$;
`, table, table, "%s", "%s", table, table)

		if l.Config.DryRun {
			log.Println("[dry-run]", query)

			continue
		}

		if _, err := tx.Exec(query); err != nil {
			return err
		}
	}

	return nil
}
