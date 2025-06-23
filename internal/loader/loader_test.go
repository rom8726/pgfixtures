package loader

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockDatabase is a mock for the db.Database interface
type MockDatabase struct {
	mock.Mock
}

func (m *MockDatabase) GetDependencyGraph(ctx context.Context, d *sql.DB) (map[string][]string, error) {
	args := m.Called(ctx, d)
	return args.Get(0).(map[string][]string), args.Error(1)
}

func (m *MockDatabase) TruncateTables(ctx context.Context, tx *sql.Tx, tables []string, dryRun bool) error {
	args := m.Called(ctx, tx, tables, dryRun)
	return args.Error(0)
}

func (m *MockDatabase) InsertRow(ctx context.Context, tx *sql.Tx, table string, row map[string]any, dryRun bool) error {
	args := m.Called(ctx, tx, table, row, dryRun)
	return args.Error(0)
}

func (m *MockDatabase) ResetSequences(ctx context.Context, tx *sql.Tx, tables []string, dryRun bool) error {
	args := m.Called(ctx, tx, tables, dryRun)
	return args.Error(0)
}

func (m *MockDatabase) Placeholder(index int) string {
	args := m.Called(index)
	return args.String(0)
}

func TestConvertIntervalSyntax(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		expected string
	}{
		{
			name:     "single day",
			expr:     "SELECT NOW() - INTERVAL '1 day'",
			expected: "SELECT NOW() - INTERVAL 1 DAY",
		},
		{
			name:     "multiple months",
			expr:     "INTERVAL '3 month'",
			expected: "INTERVAL 3 MONTH",
		},
		{
			name:     "no interval",
			expr:     "SELECT NOW()",
			expected: "SELECT NOW()",
		},
		{
			name:     "invalid interval format",
			expr:     "INTERVAL '1day'",
			expected: "INTERVAL '1day'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertIntervalSyntax(tt.expr)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestLoader_InsertRow(t *testing.T) {
	d, m, err := sqlmock.New()
	require.NoError(t, err)
	defer d.Close()

	m.ExpectBegin()
	tx, err := d.Begin()
	require.NoError(t, err)

	mockDB := &MockDatabase{}
	loader := &Loader{
		DB:       d,
		Database: mockDB,
	}

	row := map[string]any{
		"id":   1,
		"name": "$eval(SELECT 'test')",
	}
	expectedRow := map[string]any{
		"id":   1,
		"name": "test",
	}

	m.ExpectQuery("SELECT 'test'").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow("test"))
	mockDB.On("InsertRow", mock.Anything, mock.Anything, "users", expectedRow, false).Return(nil)

	err = loader.insertRow(context.Background(), tx, "users", row)
	require.NoError(t, err)

	require.NoError(t, m.ExpectationsWereMet())
	mockDB.AssertExpectations(t)
}

func TestLoader_Load(t *testing.T) {
	dir := t.TempDir()
	fixturePath := filepath.Join(dir, "fixtures.yml")
	fixtureData := `
users:
  - id: 1
    name: "test user"
posts:
  - id: 1
    title: "test post"
    user_id: 1
`
	err := os.WriteFile(fixturePath, []byte(fixtureData), 0644)
	require.NoError(t, err)

	db, dbMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	dbMock.ExpectBegin()
	dbMock.ExpectCommit()

	mockDB := &MockDatabase{}

	loader := &Loader{
		DB: db,
		Config: LoaderConfig{
			FilePath: fixturePath,
			Truncate: true,
			ResetSeq: true,
			DryRun:   false,
		},
		Database: mockDB,
	}

	mockDB.On("GetDependencyGraph", mock.Anything, mock.Anything).Return(map[string][]string{
		"posts": {"users"},
	}, nil)

	mockDB.On("TruncateTables", mock.Anything, mock.Anything, []string{"posts", "users"}, false).Return(nil)

	mockDB.On("InsertRow", mock.Anything, mock.Anything, "users", mock.AnythingOfType("map[string]interface {}"), false).Return(nil)
	mockDB.On("InsertRow", mock.Anything, mock.Anything, "posts", mock.AnythingOfType("map[string]interface {}"), false).Return(nil)

	mockDB.On("ResetSequences", mock.Anything, mock.Anything, []string{"posts", "users"}, false).Return(nil)

	err = loader.Load(context.Background())
	require.NoError(t, err)

	require.NoError(t, dbMock.ExpectationsWereMet())
	mockDB.AssertExpectations(t)
}
