package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTopoSort(t *testing.T) {
	tests := []struct {
		name      string
		graph     map[string][]string
		input     []string
		expected  []string
		expectErr bool
	}{
		{
			name:      "single_node",
			graph:     map[string][]string{"A": {}},
			input:     []string{"A"},
			expected:  []string{"A"},
			expectErr: false,
		},
		{
			name:      "simple_linear",
			graph:     map[string][]string{"A": {"B"}, "B": {"C"}, "C": {}},
			input:     []string{"A"},
			expected:  []string{"A", "B", "C"},
			expectErr: false,
		},
		{
			name:      "disconnected_nodes",
			graph:     map[string][]string{"A": {"B"}, "B": {}, "C": {}},
			input:     []string{"A", "C"},
			expected:  []string{"C", "A", "B"},
			expectErr: false,
		},
		{
			name:      "graph_with_skip_node",
			graph:     map[string][]string{"A": {"B"}, "B": {"C"}, "C": {}, "D": {}},
			input:     []string{"A"},
			expected:  []string{"A", "B", "C"},
			expectErr: false,
		},
		{
			name:      "cyclic_dependency",
			graph:     map[string][]string{"A": {"B"}, "B": {"C"}, "C": {"A"}},
			input:     []string{"A"},
			expected:  nil,
			expectErr: true,
		},
		{
			name:      "multiple_roots",
			graph:     map[string][]string{"A": {"B"}, "B": {"C"}, "D": {"E"}, "E": {}},
			input:     []string{"A", "D"},
			expected:  []string{"D", "E", "A", "B", "C"},
			expectErr: false,
		},
		{
			name:      "no_input_tables",
			graph:     map[string][]string{"A": {"B"}, "B": {"C"}, "C": {}},
			input:     []string{},
			expected:  nil,
			expectErr: false,
		},
		{
			name:      "self_dependency",
			graph:     map[string][]string{"A": {"A"}},
			input:     []string{"A"},
			expected:  nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := TopoSort(tt.graph, tt.input)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.expected == nil {
					require.Len(t, result, 0)

					return
				}

				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
