package parser

import (
	"fmt"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

var evalRe = regexp.MustCompile(`^\$eval\((.+)\)$`)

type Fixtures map[string][]map[string]any

func ParseFile(path string) (Fixtures, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	var f Fixtures
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("unmarshal yaml: %w", err)
	}

	return f, nil
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
