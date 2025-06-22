package db

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestPostgresDatabase_Placeholder(t *testing.T) {
	db := &PostgresDatabase{}
	require.Equal(t, "$1", db.Placeholder(1))
	require.Equal(t, "$2", db.Placeholder(2))
}

func TestMySQLDatabase_Placeholder(t *testing.T) {
	db := &MySQLDatabase{}
	require.Equal(t, "?", db.Placeholder(1))
	require.Equal(t, "?", db.Placeholder(2))
}

func TestPostgresDatabase_GetDependencyGraph(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT tc.table_schema").WillReturnRows(
		sqlmock.NewRows([]string{"child", "parent"}).
			AddRow("public.orders", "public.users").
			AddRow("public.orders2products", "public.orders").
			AddRow("public.orders2products", "public.products"),
	)

	d := &PostgresDatabase{}
	ctx := context.Background()
	graph, err := d.GetDependencyGraph(ctx, db)
	require.NoError(t, err)
	require.Equal(t, map[string][]string{
		"public.orders":          {"public.users"},
		"public.orders2products": {"public.orders", "public.products"},
	}, graph)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLDatabase_GetDependencyGraph(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT DATABASE\\(\\)").WillReturnRows(
		sqlmock.NewRows([]string{"DATABASE()"}).AddRow("testdb"),
	)
	mock.ExpectQuery("SELECT\\s+TABLE_NAME AS child,\\s+REFERENCED_TABLE_NAME AS parent").
		WithArgs("testdb", "testdb").
		WillReturnRows(
			sqlmock.NewRows([]string{"child", "parent"}).
				AddRow("orders", "users").
				AddRow("orders2products", "orders").
				AddRow("orders2products", "products"),
		)

	d := &MySQLDatabase{}
	ctx := context.Background()
	graph, err := d.GetDependencyGraph(ctx, db)
	require.NoError(t, err)
	require.Equal(t, map[string][]string{
		"public.orders":          {"public.users"},
		"public.orders2products": {"public.orders", "public.products"},
	}, graph)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresDatabase_TruncateTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	database := &PostgresDatabase{}
	tables := []string{"public.orders", "public.users"}

	// dryRun = true (nothing do)
	err = database.TruncateTables(context.Background(), tx, tables, true)
	require.NoError(t, err)

	// dryRun = false (will TRUNCATE)
	mock.ExpectExec("TRUNCATE public.orders, public.users RESTART IDENTITY CASCADE").WillReturnResult(sqlmock.NewResult(0, 0))
	err = database.TruncateTables(context.Background(), tx, tables, false)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLDatabase_TruncateTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	database := &MySQLDatabase{}
	tables := []string{"public.orders", "public.users"}

	// dryRun = true (nothing do, only SET FOREIGN_KEY_CHECKS)
	mock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	err = database.TruncateTables(context.Background(), tx, tables, true)
	require.NoError(t, err)

	// dryRun = false (will TRUNCATE)
	mock.ExpectBegin()
	tx2, err := db.Begin()
	require.NoError(t, err)
	mock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("TRUNCATE TABLE orders").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("TRUNCATE TABLE users").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	err = database.TruncateTables(context.Background(), tx2, tables, false)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresDatabase_InsertRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	database := &PostgresDatabase{}
	row := map[string]any{"id": 1, "name": "test"}

	// dryRun = true
	err = database.InsertRow(context.Background(), tx, "public.users", row, true)
	require.NoError(t, err)

	// dryRun = false
	mock.ExpectExec("INSERT INTO public.users \\(id, name\\) VALUES \\(\\$1, \\$2\\)").
		WithArgs(1, "test").
		WillReturnResult(sqlmock.NewResult(1, 1))
	err = database.InsertRow(context.Background(), tx, "public.users", row, false)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLDatabase_InsertRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	database := &MySQLDatabase{}
	row := map[string]any{"id": 1, "name": "test"}

	// dryRun = true
	err = database.InsertRow(context.Background(), tx, "public.users", row, true)
	require.NoError(t, err)

	// dryRun = false
	mock.ExpectExec("INSERT INTO users \\(id, name\\) VALUES \\(\\?, \\?\\)").
		WithArgs(1, "test").
		WillReturnResult(sqlmock.NewResult(1, 1))
	err = database.InsertRow(context.Background(), tx, "public.users", row, false)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
