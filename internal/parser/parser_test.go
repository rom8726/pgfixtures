package parser

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseFile(t *testing.T) {
	tests := []struct {
		name      string
		contents  string
		expected  Fixtures
		expectErr bool
	}{
		{
			name: "valid YAML",
			contents: `
public.table1:
  - key1: value1
    key2: value2
public.table2:
  - key3: value3
    key4: value4
  - key5: value5
    key6: value6
`,
			expected: Fixtures{
				"public.table1": {{"key1": "value1", "key2": "value2"}},
				"public.table2": {
					{"key3": "value3", "key4": "value4"},
					{"key5": "value5", "key6": "value6"},
				},
			},
			expectErr: false,
		},
		{
			name:      "empty file",
			contents:  "",
			expected:  Fixtures{},
			expectErr: false,
		},
		{
			name: "invalid YAML",
			contents: `
key1: value1
  key2: value2
`,
			expectErr: true,
		},
		{
			name:      "non-existent file",
			contents:  "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			filePath := filepath.Join(tempDir, "test.yaml")

			if tt.name != "non-existent file" {
				err := os.WriteFile(filePath, []byte(tt.contents), 0644)
				if err != nil {
					t.Fatalf("failed to create temp file: %v", err)
				}
			}

			actual, err := ParseFile(filePath)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, actual)
			}
		})
	}
}

func TestParseFileWithInclude(t *testing.T) {
	tempDir := t.TempDir()

	base := `
public.users:
  - id: 1
    name: Base
  - id: 2
    name: Overridden
public.products:
  - id: 1
    name: Milk
`
	_ = os.WriteFile(filepath.Join(tempDir, "base.yml"), []byte(base), 0644)

	addon := `
public.users:
  - id: 3
    name: Addon
public.products:
  - id: 2
    name: Bread
`
	_ = os.WriteFile(filepath.Join(tempDir, "addon.yml"), []byte(addon), 0644)

	main := `
include:
  - base.yml
  - addon.yml
public.users:
  - id: 2
    name: OverriddenMain
  - id: 4
    name: Main
`
	_ = os.WriteFile(filepath.Join(tempDir, "main.yml"), []byte(main), 0644)

	fixtures, err := ParseFileWithInclude(filepath.Join(tempDir, "main.yml"), map[string]bool{})
	require.NoError(t, err)
	require.Equal(t, []map[string]any{
		{"id": 1, "name": "Base"},
		{"id": 2, "name": "Overridden"},
		{"id": 3, "name": "Addon"},
		{"id": 2, "name": "OverriddenMain"},
		{"id": 4, "name": "Main"},
	}, fixtures["public.users"])
	require.Equal(t, []map[string]any{
		{"id": 1, "name": "Milk"},
		{"id": 2, "name": "Bread"},
	}, fixtures["public.products"])
}

func TestParseFileWithInclude_Nested(t *testing.T) {
	d := t.TempDir()
	_ = os.WriteFile(filepath.Join(d, "base.yml"), []byte(`public.users:
  - id: 1
    name: Base`), 0644)
	_ = os.WriteFile(filepath.Join(d, "mid.yml"), []byte(`include: base.yml
public.users:
  - id: 2
    name: Mid`), 0644)
	_ = os.WriteFile(filepath.Join(d, "main.yml"), []byte(`include: mid.yml
public.users:
  - id: 3
    name: Main`), 0644)
	fixtures, err := ParseFileWithInclude(filepath.Join(d, "main.yml"), map[string]bool{})
	require.NoError(t, err)
	require.Equal(t, []map[string]any{
		{"id": 1, "name": "Base"},
		{"id": 2, "name": "Mid"},
		{"id": 3, "name": "Main"},
	}, fixtures["public.users"])
}

func TestParseFileWithInclude_Cycle(t *testing.T) {
	d := t.TempDir()
	_ = os.WriteFile(filepath.Join(d, "a.yml"), []byte(`include: b.yml
public.users:
  - id: 1`), 0644)
	_ = os.WriteFile(filepath.Join(d, "b.yml"), []byte(`include: a.yml
public.users:
  - id: 2`), 0644)
	_, err := ParseFileWithInclude(filepath.Join(d, "a.yml"), map[string]bool{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cyclic include")
}

func TestParseFileWithInclude_EmptyInclude(t *testing.T) {
	d := t.TempDir()
	_ = os.WriteFile(filepath.Join(d, "main.yml"), []byte(`include: []
public.users:
  - id: 1`), 0644)
	fixtures, err := ParseFileWithInclude(filepath.Join(d, "main.yml"), map[string]bool{})
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{"id": 1}}, fixtures["public.users"])
}
