package parser

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

var evalRe = regexp.MustCompile(`^\$eval\((.+)\)$`)

type Fixtures map[string][]map[string]any

func ParseFile(path string) (Fixtures, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	var fixtures Fixtures
	if err := yaml.Unmarshal(data, &fixtures); err != nil {
		return nil, fmt.Errorf("unmarshal yaml: %w", err)
	}

	for table := range fixtures {
		if len(strings.Split(table, ".")) != 2 {
			return nil, fmt.Errorf("invalid fixture name (without schema): %q", table)
		}
	}

	return fixtures, nil
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
