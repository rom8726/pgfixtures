package db

import (
	"fmt"
)

func TopoSort(graph map[string][]string, inputTables []string) ([]string, error) {
	visited := make(map[string]bool)
	tempMark := make(map[string]bool)
	order := make([]string, 0, len(graph))

	include := make(map[string]bool, len(inputTables))
	for _, t := range inputTables {
		include[t] = true
	}

	var visit func(string, bool) error
	visit = func(node string, isDependent bool) error {
		if visited[node] {
			return nil
		}
		if tempMark[node] {
			return fmt.Errorf("cyclic dependency detected: %s", node)
		}

		tempMark[node] = true
		for _, dep := range graph[node] {
			if err := visit(dep, true); err != nil {
				return err
			}
		}

		tempMark[node] = false
		visited[node] = true

		if include[node] || isDependent {
			order = append(order, node)
		}

		return nil
	}

	for _, node := range inputTables {
		if err := visit(node, false); err != nil {
			return nil, err
		}
	}

	for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
		order[i], order[j] = order[j], order[i]
	}

	return order, nil
}
