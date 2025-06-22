package parser

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"gopkg.in/yaml.v3"
)

var evalRe = regexp.MustCompile(`^\$eval\((.+)\)$`)

type Fixtures map[string][]map[string]any

type rawFixtureFile struct {
	Include  any                         `yaml:"include"`
	Fixtures map[string][]map[string]any `yaml:",inline"`
}

func ParseFileWithInclude(path string, visited map[string]bool) (Fixtures, error) {
	absPath, _ := filepath.Abs(path)
	if visited[absPath] {
		return nil, fmt.Errorf("cyclic include detected: %s", absPath)
	}
	visited[absPath] = true

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	var raw rawFixtureFile
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal yaml: %w", err)
	}

	result := Fixtures{}
	if raw.Include != nil {
		var includes []string
		switch v := raw.Include.(type) {
		case string:
			includes = []string{v}
		case []any:
			for _, inc := range v {
				if s, ok := inc.(string); ok {
					includes = append(includes, s)
				}
			}
		}
		for _, incPath := range includes {
			incAbs := incPath
			if !filepath.IsAbs(incPath) {
				incAbs = filepath.Join(filepath.Dir(absPath), incPath)
			}
			incFixtures, err := ParseFileWithInclude(incAbs, visited)
			if err != nil {
				return nil, err
			}
			for table, rows := range incFixtures {
				result[table] = mergeRowsByID(result[table], rows)
			}
		}
	}
	for table, rows := range raw.Fixtures {
		result[table] = mergeRowsByID(result[table], rows)
	}
	return result, nil
}

func ParseFile(path string) (Fixtures, error) {
	return ParseFileWithInclude(path, map[string]bool{})
}

func IsEval(val any) (string, bool) {
	s, ok := val.(string)
	if !ok {
		return "", false
	}

	m := evalRe.FindStringSubmatch(s)
	if len(m) != 2 {
		return "", false
	}

	return m[1], true
}

func mergeRowsByID(slices ...[]map[string]any) []map[string]any {
	byID := map[any]map[string]any{}
	var noIDRows []map[string]any

	for _, rows := range slices {
		for _, row := range rows {
			id, hasID := row["id"]
			if hasID {
				byID[id] = row
			} else {
				noIDRows = append(noIDRows, row)
			}
		}
	}

	var result []map[string]any
	for _, row := range byID {
		result = append(result, row)
	}

	result = append(result, noIDRows...)

	return result
}
