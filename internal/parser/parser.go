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

type TemplateDef struct {
	Table   string         `yaml:"table"`
	Name    string         `yaml:"name"`
	Extends string         `yaml:"extends,omitempty"`
	Fields  map[string]any `yaml:"fields"`
}

type AllTemplates map[string]map[string]TemplateDef // table -> name -> TemplateDef

type rawFixtureFile struct {
	Include   any            `yaml:"include"`
	Templates []TemplateDef  `yaml:"templates"`
	Fixtures  map[string]any `yaml:",inline"`
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

func ParseFileWithInclude(path string, visited map[string]bool) (Fixtures, error) {
	fixtures, _, err := parseFileWithTemplatesV2(path, visited)
	return fixtures, err
}

func ParseFile(path string) (Fixtures, error) {
	return ParseFileWithInclude(path, map[string]bool{})
}

func parseFileWithTemplatesV2(path string, visited map[string]bool) (Fixtures, AllTemplates, error) {
	absPath, _ := filepath.Abs(path)
	if visited[absPath] {
		return nil, nil, fmt.Errorf("cyclic include detected: %s", absPath)
	}
	visited[absPath] = true

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read file: %w", err)
	}

	var raw rawFixtureFile
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, nil, fmt.Errorf("unmarshal yaml: %w", err)
	}

	result := Fixtures{}
	allTemplates := AllTemplates{}

	// 1. Обрабатываем include
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
			incFixtures, incTemplates, err := parseFileWithTemplatesV2(incAbs, visited)
			if err != nil {
				return nil, nil, err
			}
			for table, rows := range incFixtures {
				result[table] = mergeRowsByID(result[table], rows)
			}
			for table, tmap := range incTemplates {
				if allTemplates[table] == nil {
					allTemplates[table] = map[string]TemplateDef{}
				}
				for name, tmpl := range tmap {
					allTemplates[table][name] = tmpl
				}
			}
		}
	}

	// 2. Собираем шаблоны из текущего файла
	for _, tmpl := range raw.Templates {
		table := tmpl.Table
		if tmpl.Fields == nil {
			tmpl.Fields = map[string]any{}
		}
		if allTemplates[table] == nil {
			allTemplates[table] = map[string]TemplateDef{}
		}
		allTemplates[table][tmpl.Name] = tmpl
	}

	// 3. Собираем обычные таблицы
	for key, val := range raw.Fixtures {
		if key == "templates" {
			continue
		}
		arr, ok := val.([]any)
		if !ok {
			return nil, nil, fmt.Errorf("table %s must be an array", key)
		}
		var rows []map[string]any
		for _, v := range arr {
			row, ok := v.(map[string]any)
			if !ok {
				return nil, nil, fmt.Errorf("row in %s must be a map", key)
			}
			if _, hasExt := row["extends"]; hasExt {
				rows = append(rows, resolveExtendsV2(row, key, allTemplates))
			} else {
				rows = append(rows, deepCopyMap(row))
			}
		}
		result[key] = mergeRowsByID(result[key], rows)
	}

	return result, allTemplates, nil
}

func resolveTemplateFields(table, name string, allTemplates AllTemplates, visited map[string]bool) map[string]any {
	if visited == nil {
		visited = map[string]bool{}
	}
	key := table + ":" + name
	if visited[key] {
		panic("cyclic extends in template: " + key)
	}
	visited[key] = true

	tmpl, ok := allTemplates[table][name]
	if !ok {
		panic("template not found: " + name + " for table: " + table)
	}
	var base map[string]any
	if tmpl.Extends != "" {
		base = resolveTemplateFields(table, tmpl.Extends, allTemplates, visited)
	} else {
		base = map[string]any{}
	}
	for k, v := range tmpl.Fields {
		base[k] = v
	}
	return base
}

func resolveExtendsV2(row map[string]any, table string, allTemplates AllTemplates) map[string]any {
	ext, ok := row["extends"].(string)
	if !ok || allTemplates[table] == nil {
		return deepCopyMap(row)
	}
	base := resolveTemplateFields(table, ext, allTemplates, nil)
	merged := deepCopyMap(base)
	for k, v := range row {
		if k != "extends" {
			merged[k] = v
		}
	}
	return merged
}

func deepCopyMap(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
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
