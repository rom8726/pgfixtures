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
