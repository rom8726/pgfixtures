# pgfixtures

A Go library and CLI tool for loading fixtures into a PostgreSQL database with dynamic value support.

`pgfixtures` is a library and CLI tool for loading test data (fixtures) into PostgreSQL databases.
It's useful for setting up test environments in integration tests.

## Features

- Load data from YAML files
- Support dynamic values through `$eval()` for executing SQL queries
- Automatic table cleanup before loading (optional)
- Reset sequences after loading (optional)
- Dry-run mode to preview planned changes
- Support for foreign keys and proper loading order

## Installation

```bash
go get github.com/rom8726/pgfixtures
```

## Usage

### As a CLI Tool
```bash
pgfixtures load \
  --file fixtures.yml \
  --db "postgres://user:password@localhost:5432/dbname?sslmode=disable" \
  --truncate \
  --reset-seq
```
Flags:
- `--file, -f`: path to YAML fixtures file (default: fixtures.yml)
- `--db`: PostgreSQL connection string (required)
- `--truncate`: clean tables before loading (default: true)
- `--reset-seq`: reset sequences after loading (default: true)
- `--dry-run`: show planned changes without executing them

### As a Library
```go
import "github.com/rom8726/pgfixtures"

cfg := &pgfixtures.Config{
    FilePath: "fixtures.yml",
    ConnStr:  "postgres://user:password@localhost:5432/dbname?sslmode=disable",
    Truncate: true,
    ResetSeq: true,
    DryRun:   false,
}

err := pgfixtures.Load(cfg)
```

## Fixture Format

Fixtures are described in YAML format where top-level keys are table names:
```yaml
users:
  - id: 1
    name: "John Doe"
    created_at: $eval(SELECT NOW())
  - id: 2
    name: "Jane Doe"
    created_at: $eval(SELECT NOW() - INTERVAL '1 day')

orders:
  - id: 1
    user_id: 1
    total: 100.50
```

### Dynamic Values

Use `$eval()` construction for generating dynamic values. You can write SQL queries inside:
```yaml
users:
  - id: 1
    created_at: $eval(SELECT NOW())
    updated_at: $eval(SELECT NOW() + INTERVAL '1 hour')
    random_num: $eval(SELECT floor(random() * 100))
```

## Table Loading Order

The loading order is automatically determined based on foreign key dependencies. This ensures that referenced records exist before dependent records are inserted.

Example of proper table ordering:
1. Independent tables (no foreign keys)
2. Tables with foreign keys pointing to loaded tables
3. Junction tables (many-to-many relationships)

## Limitations

- PostgreSQL support only
- SQL queries in `$eval()` must return exactly one value
- Loading order is determined automatically based on foreign keys
