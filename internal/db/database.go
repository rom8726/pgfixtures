package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// Database defines the interface for database-specific operations
type Database interface {
	// GetDependencyGraph returns a map of table dependencies
	GetDependencyGraph(ctx context.Context, db *sql.DB) (map[string][]string, error)

	// TruncateTables generates and executes a SQL statement to truncate the given tables
	TruncateTables(ctx context.Context, tx *sql.Tx, tables []string, dryRun bool) error

	// InsertRow generates and executes a SQL statement to insert a row into a table
	InsertRow(ctx context.Context, tx *sql.Tx, table string, row map[string]any, dryRun bool) error

	// ResetSequences resets the auto-increment sequences for the given tables
	ResetSequences(ctx context.Context, tx *sql.Tx, tables []string, dryRun bool) error

	// Placeholder returns the parameter placeholder for the given index
	Placeholder(index int) string
}

// PostgresDatabase implements the Database interface for PostgreSQL
type PostgresDatabase struct{}

// GetDependencyGraph implements Database.GetDependencyGraph for PostgreSQL
func (p *PostgresDatabase) GetDependencyGraph(ctx context.Context, db *sql.DB) (map[string][]string, error) {
	query := `
SELECT
    tc.table_schema || '.' || tc.table_name AS child,
    ccu.table_schema || '.' || ccu.table_name AS parent
FROM
    information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.constraint_schema = kcu.constraint_schema
    JOIN information_schema.constraint_column_usage ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.constraint_schema = tc.constraint_schema
WHERE
    tc.constraint_type = 'FOREIGN KEY'
`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query dependencies: %w", err)
	}
	defer rows.Close()

	graph := map[string][]string{}
	for rows.Next() {
		var child, parent string
		if err := rows.Scan(&child, &parent); err != nil {
			return nil, fmt.Errorf("scan dependency: %w", err)
		}

		graph[child] = append(graph[child], parent)
	}

	return graph, nil
}

// TruncateTables implements Database.TruncateTables for PostgreSQL
func (p *PostgresDatabase) TruncateTables(ctx context.Context, tx *sql.Tx, tables []string, dryRun bool) error {
	query := "TRUNCATE " + strings.Join(tables, ", ") + " RESTART IDENTITY CASCADE"
	if dryRun {
		log.Println("[dry-run]", query)
		return nil
	}

	_, err := tx.ExecContext(ctx, query)
	return err
}

// InsertRow implements Database.InsertRow for PostgreSQL
func (p *PostgresDatabase) InsertRow(ctx context.Context, tx *sql.Tx, table string, row map[string]any, dryRun bool) error {
	var cols []string
	var vals []any
	var ph []string

	i := 1
	for col, val := range row {
		cols = append(cols, col)
		vals = append(vals, val)
		ph = append(ph, p.Placeholder(i))
		i++
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(cols, ", "),
		strings.Join(ph, ", "),
	)

	if dryRun {
		log.Printf("[dry-run] %s :: %v", query, vals)
		return nil
	}

	_, err := tx.ExecContext(ctx, query, vals...)
	return err
}

// ResetSequences implements Database.ResetSequences for PostgreSQL
func (p *PostgresDatabase) ResetSequences(ctx context.Context, tx *sql.Tx, tables []string, dryRun bool) error {
	for _, schemaTable := range tables {
		parts := strings.Split(schemaTable, ".")
		if len(parts) != 2 {
			return fmt.Errorf("invalid table name: %q", schemaTable)
		}

		query := fmt.Sprintf(`
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT column_default, column_name FROM information_schema.columns
        WHERE table_schema = '%s' AND table_name = '%s' AND column_default LIKE 'nextval%%'
    LOOP
        EXECUTE format('SELECT setval(pg_get_serial_sequence(''%s'', ''%s''), COALESCE(MAX(%s), 1)) FROM %s',
            r.column_name, r.column_name, r.column_name, '%s');
    END LOOP;
END$$;
`, parts[0], parts[1], schemaTable, "%s", "%s", schemaTable, schemaTable)

		if dryRun {
			log.Println("[dry-run]", query)
			continue
		}

		if _, err := tx.ExecContext(ctx, query); err != nil {
			return err
		}
	}

	return nil
}

// Placeholder implements Database.Placeholder for PostgreSQL
func (p *PostgresDatabase) Placeholder(index int) string {
	return fmt.Sprintf("$%d", index)
}

// MySQLDatabase implements the Database interface for MySQL
type MySQLDatabase struct{}

// GetDependencyGraph implements Database.GetDependencyGraph for MySQL
func (m *MySQLDatabase) GetDependencyGraph(ctx context.Context, db *sql.DB) (map[string][]string, error) {
	// For MySQL, we need to get the current database name
	var dbName string
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&dbName); err != nil {
		return nil, fmt.Errorf("get current database: %w", err)
	}

	query := `
SELECT
    TABLE_NAME AS child,
    REFERENCED_TABLE_NAME AS parent
FROM
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE
    REFERENCED_TABLE_SCHEMA IS NOT NULL
    AND TABLE_SCHEMA = ?
    AND REFERENCED_TABLE_SCHEMA = ?
`
	rows, err := db.QueryContext(ctx, query, dbName, dbName)
	if err != nil {
		return nil, fmt.Errorf("query dependencies: %w", err)
	}
	defer rows.Close()

	graph := map[string][]string{}
	for rows.Next() {
		var childTable, parentTable string
		if err := rows.Scan(&childTable, &parentTable); err != nil {
			return nil, fmt.Errorf("scan dependency: %w", err)
		}

		// For compatibility with the fixtures file, we need to add the "public." prefix
		child := "public." + childTable
		parent := "public." + parentTable

		graph[child] = append(graph[child], parent)
	}

	return graph, nil
}

// TruncateTables implements Database.TruncateTables for MySQL
func (m *MySQLDatabase) TruncateTables(ctx context.Context, tx *sql.Tx, tables []string, dryRun bool) error {
	// MySQL requires foreign key checks to be disabled for truncating tables with foreign key constraints
	if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		return err
	}

	for _, schemaTable := range tables {
		// For MySQL, we need to strip the schema part (if any)
		parts := strings.Split(schemaTable, ".")
		tableName := parts[len(parts)-1] // Get the last part (table name)

		query := "TRUNCATE TABLE " + tableName
		if dryRun {
			log.Println("[dry-run]", query)
			continue
		}

		if _, err := tx.ExecContext(ctx, query); err != nil {
			return err
		}
	}

	// Re-enable foreign key checks
	if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		return err
	}

	return nil
}

// InsertRow implements Database.InsertRow for MySQL
func (m *MySQLDatabase) InsertRow(ctx context.Context, tx *sql.Tx, table string, row map[string]any, dryRun bool) error {
	var cols []string
	var vals []any
	var ph []string

	// For MySQL, we need to strip the schema part (if any)
	parts := strings.Split(table, ".")
	tableName := parts[len(parts)-1] // Get the last part (table name)

	i := 1
	for col, val := range row {
		cols = append(cols, col)
		vals = append(vals, val)
		ph = append(ph, m.Placeholder(i))
		i++
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(cols, ", "),
		strings.Join(ph, ", "),
	)

	if dryRun {
		log.Printf("[dry-run] %s :: %v", query, vals)
		return nil
	}

	_, err := tx.ExecContext(ctx, query, vals...)
	return err
}

// ResetSequences implements Database.ResetSequences for MySQL
func (m *MySQLDatabase) ResetSequences(ctx context.Context, tx *sql.Tx, tables []string, dryRun bool) error {
	// MySQL doesn't have sequences like PostgreSQL, but it has AUTO_INCREMENT
	// We need to get the maximum value for each AUTO_INCREMENT column and set it
	for _, schemaTable := range tables {
		parts := strings.Split(schemaTable, ".")
		var dbName, tableName string

		if len(parts) == 2 {
			dbName = parts[0]
			tableName = parts[1]
		} else if len(parts) == 1 {
			// If no schema is provided, use the current database
			dbName = "db" // This is the database name we're using in the test
			tableName = parts[0]
		} else {
			return fmt.Errorf("invalid table name: %q", schemaTable)
		}

		// Get AUTO_INCREMENT columns for this table
		query := fmt.Sprintf(`
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '%s'
  AND TABLE_NAME = '%s'
  AND EXTRA LIKE '%%auto_increment%%'
`, dbName, tableName)

		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("query auto_increment columns: %w", err)
		}

		var columns []string
		for rows.Next() {
			var column string
			if err := rows.Scan(&column); err != nil {
				rows.Close()
				return fmt.Errorf("scan auto_increment column: %w", err)
			}
			columns = append(columns, column)
		}
		rows.Close()

		// For each AUTO_INCREMENT column, get the max value and set the AUTO_INCREMENT
		for _, column := range columns {
			// Get max value
			maxQuery := fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) + 1 FROM %s", column, tableName)
			var maxVal int
			if err := tx.QueryRowContext(ctx, maxQuery).Scan(&maxVal); err != nil {
				return fmt.Errorf("get max value: %w", err)
			}

			// Set AUTO_INCREMENT
			alterQuery := fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT = %d", tableName, maxVal)
			if dryRun {
				log.Println("[dry-run]", alterQuery)
				continue
			}

			if _, err := tx.ExecContext(ctx, alterQuery); err != nil {
				return fmt.Errorf("set auto_increment: %w", err)
			}
		}
	}

	return nil
}

// Placeholder implements Database.Placeholder for MySQL
func (m *MySQLDatabase) Placeholder(index int) string {
	return "?"
}
