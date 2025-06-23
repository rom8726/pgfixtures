package parser

import (
	"os"
	"path/filepath"
	"sort"
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
	users := fixtures["public.users"]
	sort.Slice(users, func(i, j int) bool {
		return users[i]["id"].(int) < users[j]["id"].(int)
	})
	require.Equal(t, []map[string]any{
		{"id": 1, "name": "Base"},
		{"id": 2, "name": "OverriddenMain"},
		{"id": 3, "name": "Addon"},
		{"id": 4, "name": "Main"},
	}, users)
	products := fixtures["public.products"]
	sort.Slice(products, func(i, j int) bool {
		return products[i]["id"].(int) < products[j]["id"].(int)
	})
	require.Equal(t, []map[string]any{
		{"id": 1, "name": "Milk"},
		{"id": 2, "name": "Bread"},
	}, products)
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
	users := fixtures["public.users"]
	sort.Slice(users, func(i, j int) bool {
		return users[i]["id"].(int) < users[j]["id"].(int)
	})
	require.Equal(t, []map[string]any{
		{"id": 1, "name": "Base"},
		{"id": 2, "name": "Mid"},
		{"id": 3, "name": "Main"},
	}, users)
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

func TestParseFileWithInclude_Extends(t *testing.T) {
	d := t.TempDir()
	_ = os.WriteFile(filepath.Join(d, "base.yml"), []byte(`templates:
  - table: public.users
    name: base
    fields:
      name: Base User
      created_at: $eval(SELECT NOW())
  - table: public.users
    name: admin
    extends: base
    fields:
      name: Admin User
      is_admin: true
public.users:
  - id: 1
    extends: base
    email: user1@example.com
  - id: 2
    extends: admin
    email: admin@example.com
`), 0644)
	fixtures, err := ParseFileWithInclude(filepath.Join(d, "base.yml"), map[string]bool{})
	require.NoError(t, err)
	users := fixtures["public.users"]
	require.Len(t, users, 2)
	sort.Slice(users, func(i, j int) bool {
		return users[i]["id"].(int) < users[j]["id"].(int)
	})
	require.Equal(t, 1, users[0]["id"])
	require.Equal(t, "Base User", users[0]["name"])
	require.Equal(t, "user1@example.com", users[0]["email"])
	require.Contains(t, users[0], "created_at")
	require.Equal(t, 2, users[1]["id"])
	require.Equal(t, "Admin User", users[1]["name"])
	require.Equal(t, true, users[1]["is_admin"])
	require.Equal(t, "admin@example.com", users[1]["email"])
	require.Contains(t, users[1], "created_at")
}

func TestParseFileWithInclude_ExtendsWithInclude(t *testing.T) {
	d := t.TempDir()
	_ = os.WriteFile(filepath.Join(d, "base.yml"), []byte(`templates:
  - table: public.users
    name: base
    fields:
      name: Base User
      created_at: $eval(SELECT NOW())
public.users:
  - id: 1
    extends: base
    email: user1@example.com
`), 0644)
	_ = os.WriteFile(filepath.Join(d, "main.yml"), []byte(`include: base.yml
templates:
  - table: public.users
    name: admin
    extends: base
    fields:
      name: Admin User
      is_admin: true
public.users:
  - id: 2
    extends: admin
    email: admin@example.com
`), 0644)
	fixtures, err := ParseFileWithInclude(filepath.Join(d, "main.yml"), map[string]bool{})
	require.NoError(t, err)
	users := fixtures["public.users"]
	require.Len(t, users, 2)
	sort.Slice(users, func(i, j int) bool {
		return users[i]["id"].(int) < users[j]["id"].(int)
	})
	require.Equal(t, 1, users[0]["id"])
	require.Equal(t, "Base User", users[0]["name"])
	require.Equal(t, "user1@example.com", users[0]["email"])
	require.Equal(t, 2, users[1]["id"])
	require.Equal(t, "Admin User", users[1]["name"])
	require.Equal(t, true, users[1]["is_admin"])
	require.Equal(t, "admin@example.com", users[1]["email"])
}

func TestParseFileWithInclude_ExtendsRecursive(t *testing.T) {
	d := t.TempDir()
	_ = os.WriteFile(filepath.Join(d, "main.yml"), []byte(`templates:
  - table: public.users
    name: base
    fields:
      name: Base User
      created_at: $eval(SELECT NOW())
  - table: public.users
    name: admin
    extends: base
    fields:
      name: Admin User
      is_admin: true
  - table: public.users
    name: superadmin
    extends: admin
    fields:
      name: Super Admin
      super: true
public.users:
  - id: 1
    extends: superadmin
    email: super@example.com
`), 0644)
	fixtures, err := ParseFileWithInclude(filepath.Join(d, "main.yml"), map[string]bool{})
	require.NoError(t, err)
	users := fixtures["public.users"]
	sort.Slice(users, func(i, j int) bool {
		return users[i]["id"].(int) < users[j]["id"].(int)
	})
	require.Len(t, users, 1)
	require.Equal(t, 1, users[0]["id"])
	require.Equal(t, "Super Admin", users[0]["name"])
	require.Equal(t, true, users[0]["is_admin"])
	require.Equal(t, true, users[0]["super"])
	require.Equal(t, "super@example.com", users[0]["email"])
	require.Contains(t, users[0], "created_at")
}

func TestParseFileWithInclude_ExtendsOverrideFields(t *testing.T) {
	d := t.TempDir()
	_ = os.WriteFile(filepath.Join(d, "main.yml"), []byte(`templates:
  - table: public.users
    name: base
    fields:
      name: Base User
      created_at: $eval(SELECT NOW())
public.users:
  - id: 1
    extends: base
    name: Overridden
    email: test@example.com
`), 0644)
	fixtures, err := ParseFileWithInclude(filepath.Join(d, "main.yml"), map[string]bool{})
	require.NoError(t, err)
	users := fixtures["public.users"]
	require.Len(t, users, 1)
	require.Equal(t, 1, users[0]["id"])
	require.Equal(t, "Overridden", users[0]["name"])
	require.Equal(t, "test@example.com", users[0]["email"])
	require.Contains(t, users[0], "created_at")
}

func TestIsEval(t *testing.T) {
	tests := []struct {
		name  string
		input any
		expr  string
		ok    bool
	}{
		{
			name:  "valid eval",
			input: "$eval(SELECT 1)",
			expr:  "SELECT 1",
			ok:    true,
		},
		{
			name:  "not a string",
			input: 123,
			expr:  "",
			ok:    false,
		},
		{
			name:  "no eval prefix",
			input: "SELECT 1",
			expr:  "",
			ok:    false,
		},
		{
			name:  "malformed eval",
			input: "$evalSELECT 1)",
			expr:  "",
			ok:    false,
		},
		{
			name:  "empty eval",
			input: "$eval()",
			expr:  "",
			ok:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, ok := IsEval(tt.input)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.expr, expr)
		})
	}
}
