# pgfixtures

A Go library and CLI tool for loading fixtures into PostgreSQL and MySQL databases with dynamic value support.

`pgfixtures` is a library and CLI tool for loading test data (fixtures) into databases.
It's useful for setting up test environments in integration tests.

## Features

- Load data from YAML files
- Support for both PostgreSQL and MySQL databases
- Support dynamic values through `$eval()` for executing SQL queries
- Automatic table cleanup before loading (optional)
- Reset sequences after loading (optional)
- Dry-run mode to preview planned changes
- Support for foreign keys and proper loading order
- **Fixture templates and inheritance via `include` with merge by `id`**

⚠️ **NOTE: Please, point table schema for each table in YAML fixture for correct toposort (for example, public.test)**

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
  --db-type postgres \
  --truncate \
  --reset-seq
```

For MySQL:
```bash
pgfixtures load \
  --file fixtures.yml \
  --db "user:password@tcp(localhost:3306)/dbname" \
  --db-type mysql \
  --truncate \
  --reset-seq
```

Flags:
- `--file, -f`: path to YAML fixtures file (default: fixtures.yml)
- `--db`: database connection string (required)
- `--db-type`: database type (postgres or mysql, default: postgres)
- `--truncate`: clean tables before loading (default: true)
- `--reset-seq`: reset sequences after loading (default: true)
- `--dry-run`: show planned changes without executing them

### As a Library
```go
import (
    "github.com/rom8726/pgfixtures"
)

// For PostgreSQL
pgCfg := &pgfixtures.Config{
    FilePath:     "fixtures.yml",
    ConnStr:      "postgres://user:password@localhost:5432/dbname?sslmode=disable",
    DatabaseType: db.PostgreSQL, // Default if not specified
    Truncate:     true,
    ResetSeq:     true,
    DryRun:       false,
}

err := pgfixtures.Load(context.Background(), pgCfg)

// For MySQL
myCfg := &pgfixtures.Config{
    FilePath:     "fixtures.yml",
    ConnStr:      "user:password@tcp(localhost:3306)/dbname",
    DatabaseType: db.MySQL,
    Truncate:     true,
    ResetSeq:     true,
    DryRun:       false,
}

err = pgfixtures.Load(context.Background(), myCfg)
```

## Fixture Format

Fixtures are described in YAML format where top-level keys are table names:
```yaml
public.users:
  - id: 1
    name: "John Doe"
    created_at: $eval(SELECT NOW())
  - id: 2
    name: "Jane Doe"
    created_at: $eval(SELECT NOW() - INTERVAL '1 day')

public.orders:
  - id: 1
    user_id: 1
    total: 100.50
```

### Dynamic Values

Use `$eval()` construction for generating dynamic values. You can write SQL queries inside:
```yaml
public.users:
  - id: 1
    created_at: $eval(SELECT NOW())
    updated_at: $eval(SELECT NOW() + INTERVAL '1 hour')
    random_num: $eval(SELECT floor(random() * 100))
```

### Fixture Templates, Inheritance and Merge by id

You can split your fixtures into reusable templates and include them in your main fixture file using the `include` key. You can include one or multiple files:

```yaml
# base.yml
public.users:
  - id: 1
    name: "Base User"
  - id: 2
    name: "Template User"

# addon.yml
public.users:
  - id: 2
    name: "Addon User"
  - id: 3
    name: "Addon User 2"

# main.yml
include:
  - base.yml
  - addon.yml
public.users:
  - id: 2
    name: "Overridden User"
  - id: 4
    name: "Main User"
```

**How it works:**
- All included files are merged in order.
- For each table, rows are merged by `id` (if present): if the same `id` appears in several files, the last one wins (the main file overrides included templates).
- As a result, in the example above, the final `public.users` will contain:
  - id: 1, name: "Base User"
  - id: 2, name: "Overridden User"   # from main.yml, overrides all previous
  - id: 3, name: "Addon User 2"
  - id: 4, name: "Main User"

If a row does not have an `id` field, it is simply appended.

### Table Loading Order

The loading order is automatically determined based on foreign key dependencies. This ensures that referenced records exist before dependent records are inserted.

Example of proper table ordering:
1. Independent tables (no foreign keys)
2. Tables with foreign keys pointing to loaded tables
3. Junction tables (many-to-many relationships)

## Limitations

- SQL queries in `$eval()` must return exactly one value
- Loading order is determined automatically based on foreign keys
- MySQL support is new and may have some edge cases not fully covered
- Only the `id` field is used for merging rows when using `include` (if present)
