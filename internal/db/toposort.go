package db

import (
	"context"
	"database/sql"
	"fmt"
)

func GetDependencyGraph(ctx context.Context, db *sql.DB) (map[string][]string, error) {
	query := `
SELECT
    tc.table_schema || '.' || tc.table_name AS child,
    ccu.table_schema || '.' || ccu.table_name AS parent
FROM
    information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.constraint_schema = kcu.constraint_schema
    JOIN information_schema.constraint_column_usage ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.constraint_schema = tc.constraint_schema
WHERE
    tc.constraint_type = 'FOREIGN KEY'
`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query dependencies: %w", err)
	}
	defer rows.Close()

	graph := map[string][]string{}
	for rows.Next() {
		var child, parent string
		if err := rows.Scan(&child, &parent); err != nil {
			return nil, fmt.Errorf("scan dependency: %w", err)
		}

		graph[child] = append(graph[child], parent)
	}

	return graph, nil
}

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
