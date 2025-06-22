package pgfixtures

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type user struct {
	ID          int
	Name        string
	LastLoginAt time.Time
	CreatedAt   time.Time
}

type order struct {
	ID        int
	UserID    int
	CreatedAt time.Time
}

type product struct {
	ID    int
	Name  string
	Price float64
}

type orderProduct struct {
	OrderID   int
	ProductID int
	Quantity  int
	Price     float64
}

func TestLoadPostgreSQL__simple_one_file(t *testing.T) {
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase("db"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(15*time.Second),
		),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	time.Sleep(5 * time.Second)

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	cfg := &Config{
		FilePath:     "testdata/fixtures_01.yml",
		ConnStr:      connStr,
		DatabaseType: PostgreSQL,
		Truncate:     true,
		ResetSeq:     true,
		DryRun:       false,
	}

	// read migrations
	migrationSQL, err := os.ReadFile("testdata/migration_postgresql.sql")
	require.NoError(t, err, "read migrations")

	// open connect to DB
	db, err := sql.Open("postgres", cfg.ConnStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// apply migrations
	_, err = db.Exec(string(migrationSQL))
	require.NoError(t, err, "apply migrations")

	// load fixtures
	require.NoError(t, Load(context.Background(), cfg), "load fixtures")

	// check users
	rows, err := db.Query("SELECT id, name, last_login_at, created_at FROM users ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var users []user
	for rows.Next() {
		var u user
		require.NoError(t, rows.Scan(&u.ID, &u.Name, &u.LastLoginAt, &u.CreatedAt))
		users = append(users, u)
	}
	require.NoError(t, rows.Err())
	require.Len(t, users, 2)

	// user user 1
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, "User1", users[0].Name)
	require.True(t, users[0].LastLoginAt.After(users[0].CreatedAt))

	// check user 2
	require.Equal(t, 2, users[1].ID)
	require.Equal(t, "User2", users[1].Name)
	require.True(t, users[1].LastLoginAt.After(users[1].CreatedAt))

	// check orders
	rows, err = db.Query("SELECT id, user_id, created_at FROM orders ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var orders []order
	for rows.Next() {
		var o order
		require.NoError(t, rows.Scan(&o.ID, &o.UserID, &o.CreatedAt))
		orders = append(orders, o)
	}
	require.NoError(t, rows.Err())
	require.Len(t, orders, 2)

	require.Equal(t, 1, orders[0].ID)
	require.Equal(t, 1, orders[0].UserID)
	require.Equal(t, 2, orders[1].ID)
	require.Equal(t, 2, orders[1].UserID)

	// check products
	rows, err = db.Query("SELECT id, name, price FROM products ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var products []product
	for rows.Next() {
		var p product
		require.NoError(t, rows.Scan(&p.ID, &p.Name, &p.Price))
		products = append(products, p)
	}
	require.NoError(t, rows.Err())
	require.Len(t, products, 3)

	expectedProducts := []product{
		{ID: 1, Name: "Milk", Price: 2.50},
		{ID: 2, Name: "Bread", Price: 1.80},
		{ID: 3, Name: "Phone", Price: 399.99},
	}
	for i, expected := range expectedProducts {
		require.Equal(t, expected.ID, products[i].ID)
		require.Equal(t, expected.Name, products[i].Name)
		require.InEpsilon(t, expected.Price, products[i].Price, 0.0001)
	}

	// check orders products
	rows, err = db.Query("SELECT order_id, product_id, quantity, price FROM orders2products ORDER BY order_id, product_id")
	require.NoError(t, err)
	defer rows.Close()

	var orderProducts []orderProduct
	for rows.Next() {
		var op orderProduct
		require.NoError(t, rows.Scan(&op.OrderID, &op.ProductID, &op.Quantity, &op.Price))
		orderProducts = append(orderProducts, op)
	}
	require.NoError(t, rows.Err())
	require.Len(t, orderProducts, 3)

	expectedOrderProducts := []orderProduct{
		{OrderID: 1, ProductID: 2, Quantity: 2, Price: 3.60},
		{OrderID: 2, ProductID: 1, Quantity: 1, Price: 2.50},
		{OrderID: 2, ProductID: 3, Quantity: 1, Price: 399.99},
	}
	for i, expected := range expectedOrderProducts {
		require.Equal(t, expected.OrderID, orderProducts[i].OrderID)
		require.Equal(t, expected.ProductID, orderProducts[i].ProductID)
		require.Equal(t, expected.Quantity, orderProducts[i].Quantity)
		require.InEpsilon(t, expected.Price, orderProducts[i].Price, 0.0001)
	}
}

func TestLoadMySQL__simple_one_file(t *testing.T) {
	ctx := context.Background()

	// Start a MySQL container
	mysqlContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mysql:8.0",
			ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{
				"MYSQL_ROOT_PASSWORD": "password",
				"MYSQL_DATABASE":      "db",
				"MYSQL_USER":          "user",
				"MYSQL_PASSWORD":      "password",
			},
			WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").
				WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := mysqlContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	time.Sleep(5 * time.Second)

	// Get connection details
	host, err := mysqlContainer.Host(ctx)
	require.NoError(t, err)

	port, err := mysqlContainer.MappedPort(ctx, "3306/tcp")
	require.NoError(t, err)

	// Create a connection string with multiStatements=true to allow executing multiple statements at once
	// Use root user to ensure we have all necessary privileges
	// Add parseTime=true to convert MySQL timestamps to time.Time
	connStr := fmt.Sprintf("root:password@tcp(%s:%s)/db?multiStatements=true&parseTime=true", host, port.Port())

	cfg := &Config{
		FilePath:     "testdata/fixtures_01.yml",
		ConnStr:      connStr,
		DatabaseType: MySQL,
		Truncate:     true,
		ResetSeq:     true,
		DryRun:       false,
	}

	// read migrations
	migrationSQL, err := os.ReadFile("testdata/migration_mysql.sql")
	require.NoError(t, err, "read migrations")

	// open connect to DB
	db, err := sql.Open("mysql", cfg.ConnStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// apply migrations
	_, err = db.Exec(string(migrationSQL))
	require.NoError(t, err, "apply migrations")

	// load fixtures
	require.NoError(t, Load(context.Background(), cfg), "load fixtures")

	// check users
	rows, err := db.Query("SELECT id, name, last_login_at, created_at FROM users ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var users []user
	for rows.Next() {
		var u user
		require.NoError(t, rows.Scan(&u.ID, &u.Name, &u.LastLoginAt, &u.CreatedAt))
		users = append(users, u)
	}
	require.NoError(t, rows.Err())
	require.Len(t, users, 2)

	// user user 1
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, "User1", users[0].Name)
	require.True(t, users[0].LastLoginAt.After(users[0].CreatedAt))

	// check user 2
	require.Equal(t, 2, users[1].ID)
	require.Equal(t, "User2", users[1].Name)
	require.True(t, users[1].LastLoginAt.After(users[1].CreatedAt))

	// check orders
	rows, err = db.Query("SELECT id, user_id, created_at FROM orders ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var orders []order
	for rows.Next() {
		var o order
		require.NoError(t, rows.Scan(&o.ID, &o.UserID, &o.CreatedAt))
		orders = append(orders, o)
	}
	require.NoError(t, rows.Err())
	require.Len(t, orders, 2)

	require.Equal(t, 1, orders[0].ID)
	require.Equal(t, 1, orders[0].UserID)
	require.Equal(t, 2, orders[1].ID)
	require.Equal(t, 2, orders[1].UserID)

	// check products
	rows, err = db.Query("SELECT id, name, price FROM products ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var products []product
	for rows.Next() {
		var p product
		require.NoError(t, rows.Scan(&p.ID, &p.Name, &p.Price))
		products = append(products, p)
	}
	require.NoError(t, rows.Err())
	require.Len(t, products, 3)

	expectedProducts := []product{
		{ID: 1, Name: "Milk", Price: 2.50},
		{ID: 2, Name: "Bread", Price: 1.80},
		{ID: 3, Name: "Phone", Price: 399.99},
	}
	for i, expected := range expectedProducts {
		require.Equal(t, expected.ID, products[i].ID)
		require.Equal(t, expected.Name, products[i].Name)
		require.InEpsilon(t, expected.Price, products[i].Price, 0.0001)
	}

	// check orders products
	rows, err = db.Query("SELECT order_id, product_id, quantity, price FROM orders2products ORDER BY order_id, product_id")
	require.NoError(t, err)
	defer rows.Close()

	var orderProducts []orderProduct
	for rows.Next() {
		var op orderProduct
		require.NoError(t, rows.Scan(&op.OrderID, &op.ProductID, &op.Quantity, &op.Price))
		orderProducts = append(orderProducts, op)
	}
	require.NoError(t, rows.Err())
	require.Len(t, orderProducts, 3)

	expectedOrderProducts := []orderProduct{
		{OrderID: 1, ProductID: 2, Quantity: 2, Price: 3.60},
		{OrderID: 2, ProductID: 1, Quantity: 1, Price: 2.50},
		{OrderID: 2, ProductID: 3, Quantity: 1, Price: 399.99},
	}
	for i, expected := range expectedOrderProducts {
		require.Equal(t, expected.OrderID, orderProducts[i].OrderID)
		require.Equal(t, expected.ProductID, orderProducts[i].ProductID)
		require.Equal(t, expected.Quantity, orderProducts[i].Quantity)
		require.InEpsilon(t, expected.Price, orderProducts[i].Price, 0.0001)
	}
}

func TestLoadPostgreSQL__include_templates(t *testing.T) {
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase("db"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(15*time.Second),
		),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	time.Sleep(5 * time.Second)

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	cfg := &Config{
		FilePath:     "testdata/fixtures_include_main.yml",
		ConnStr:      connStr,
		DatabaseType: PostgreSQL,
		Truncate:     true,
		ResetSeq:     true,
		DryRun:       false,
	}

	migrationSQL, err := os.ReadFile("testdata/migration_postgresql.sql")
	require.NoError(t, err, "read migrations")

	db, err := sql.Open("postgres", cfg.ConnStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(string(migrationSQL))
	require.NoError(t, err, "apply migrations")

	require.NoError(t, Load(context.Background(), cfg), "load fixtures")

	rows, err := db.Query("SELECT id, name FROM users ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var users []user
	for rows.Next() {
		var u user
		require.NoError(t, rows.Scan(&u.ID, &u.Name))
		users = append(users, u)
	}
	require.NoError(t, rows.Err())
	require.Len(t, users, 3)
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, "BaseUser", users[0].Name)
	require.Equal(t, 2, users[1].ID)
	require.Equal(t, "OverriddenUser", users[1].Name)
	require.Equal(t, 3, users[2].ID)
	require.Equal(t, "MainUser", users[2].Name)

	rows, err = db.Query("SELECT id, name, price FROM products ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var products []product
	for rows.Next() {
		var p product
		require.NoError(t, rows.Scan(&p.ID, &p.Name, &p.Price))
		products = append(products, p)
	}
	require.NoError(t, rows.Err())
	require.Len(t, products, 3)
	require.Equal(t, 1, products[0].ID)
	require.Equal(t, "Milk", products[0].Name)
	require.InEpsilon(t, 2.50, products[0].Price, 0.0001)

	require.Equal(t, 2, products[1].ID)
	require.Equal(t, "Bread", products[1].Name)
	require.InEpsilon(t, 1.80, products[1].Price, 0.0001)

	require.Equal(t, 3, products[2].ID)
	require.Equal(t, "Phone", products[2].Name)
	require.InEpsilon(t, 399.99, products[2].Price, 0.0001)
}

func TestLoadMySQL__include_templates(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mysql:8.0",
			ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{
				"MYSQL_ROOT_PASSWORD": "password",
				"MYSQL_DATABASE":      "db",
				"MYSQL_USER":          "user",
				"MYSQL_PASSWORD":      "password",
			},
			WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").
				WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := mysqlContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	time.Sleep(5 * time.Second)

	host, err := mysqlContainer.Host(ctx)
	require.NoError(t, err)

	port, err := mysqlContainer.MappedPort(ctx, "3306/tcp")
	require.NoError(t, err)

	connStr := fmt.Sprintf("root:password@tcp(%s:%s)/db?multiStatements=true&parseTime=true", host, port.Port())

	cfg := &Config{
		FilePath:     "testdata/fixtures_include_main.yml",
		ConnStr:      connStr,
		DatabaseType: MySQL,
		Truncate:     true,
		ResetSeq:     true,
		DryRun:       false,
	}

	migrationSQL, err := os.ReadFile("testdata/migration_mysql.sql")
	require.NoError(t, err, "read migrations")

	db, err := sql.Open("mysql", cfg.ConnStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(string(migrationSQL))
	require.NoError(t, err, "apply migrations")

	require.NoError(t, Load(context.Background(), cfg), "load fixtures")

	rows, err := db.Query("SELECT id, name FROM users ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var users []user
	for rows.Next() {
		var u user
		require.NoError(t, rows.Scan(&u.ID, &u.Name))
		users = append(users, u)
	}
	require.NoError(t, rows.Err())
	require.Len(t, users, 3)
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, "BaseUser", users[0].Name)
	require.Equal(t, 2, users[1].ID)
	require.Equal(t, "OverriddenUser", users[1].Name)
	require.Equal(t, 3, users[2].ID)
	require.Equal(t, "MainUser", users[2].Name)

	rows, err = db.Query("SELECT id, name, price FROM products ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var products []product
	for rows.Next() {
		var p product
		require.NoError(t, rows.Scan(&p.ID, &p.Name, &p.Price))
		products = append(products, p)
	}
	require.NoError(t, rows.Err())
	require.Len(t, products, 3)
	require.Equal(t, 1, products[0].ID)
	require.Equal(t, "Milk", products[0].Name)
	require.InEpsilon(t, 2.50, products[0].Price, 0.0001)

	require.Equal(t, 2, products[1].ID)
	require.Equal(t, "Bread", products[1].Name)
	require.InEpsilon(t, 1.80, products[1].Price, 0.0001)

	require.Equal(t, 3, products[2].ID)
	require.Equal(t, "Phone", products[2].Name)
	require.InEpsilon(t, 399.99, products[2].Price, 0.0001)
}

func TestLoadPostgreSQL__include_and_extends_templates(t *testing.T) {
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase("db"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second),
		),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	time.Sleep(5 * time.Second)

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	cfg := &Config{
		FilePath:     "testdata/fixtures_templates_integration_main.yml",
		ConnStr:      connStr,
		DatabaseType: PostgreSQL,
		Truncate:     true,
		ResetSeq:     true,
		DryRun:       false,
	}

	migrationSQL, err := os.ReadFile("testdata/migration_postgresql.sql")
	require.NoError(t, err, "read migrations")

	db, err := sql.Open("postgres", cfg.ConnStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(string(migrationSQL))
	require.NoError(t, err, "apply migrations")

	require.NoError(t, Load(context.Background(), cfg), "load fixtures")

	rows, err := db.Query("SELECT id, name, email, is_admin, super FROM users ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	type user struct {
		ID      int
		Name    string
		Email   string
		IsAdmin sql.NullBool
		Super   sql.NullBool
	}
	var users []user
	for rows.Next() {
		var u user
		require.NoError(t, rows.Scan(&u.ID, &u.Name, &u.Email, &u.IsAdmin, &u.Super))
		users = append(users, u)
	}
	require.NoError(t, rows.Err())
	require.Len(t, users, 4)

	require.Equal(t, user{ID: 1, Name: "Base User", Email: "user1@example.com"}, users[0])
	require.Equal(t, user{ID: 2, Name: "Admin User", Email: "admin@example.com", IsAdmin: sql.NullBool{Bool: true, Valid: true}}, users[1])
	require.Equal(t, user{ID: 3, Name: "Super Admin", Email: "superadmin@example.com", IsAdmin: sql.NullBool{Bool: true, Valid: true}, Super: sql.NullBool{Bool: true, Valid: true}}, users[2])
	require.Equal(t, user{ID: 4, Name: "NoTemplate", Email: "notemplate@example.com"}, users[3])
}

func TestLoadMySQL__include_templates_complex(t *testing.T) {
	ctx := context.Background()

	mysqlContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mysql:8.0",
			ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{
				"MYSQL_ROOT_PASSWORD": "password",
				"MYSQL_DATABASE":      "db",
				"MYSQL_USER":          "user",
				"MYSQL_PASSWORD":      "password",
			},
			WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").
				WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := mysqlContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	time.Sleep(5 * time.Second)

	host, err := mysqlContainer.Host(ctx)
	require.NoError(t, err)

	port, err := mysqlContainer.MappedPort(ctx, "3306/tcp")
	require.NoError(t, err)

	connStr := fmt.Sprintf("root:password@tcp(%s:%s)/db?multiStatements=true&parseTime=true", host, port.Port())

	cfg := &Config{
		FilePath:     "testdata/fixtures_include_main.yml",
		ConnStr:      connStr,
		DatabaseType: MySQL,
		Truncate:     true,
		ResetSeq:     true,
		DryRun:       false,
	}

	migrationSQL, err := os.ReadFile("testdata/migration_mysql.sql")
	require.NoError(t, err, "read migrations")

	db, err := sql.Open("mysql", cfg.ConnStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(string(migrationSQL))
	require.NoError(t, err, "apply migrations")

	require.NoError(t, Load(context.Background(), cfg), "load fixtures")

	rows, err := db.Query("SELECT id, name FROM users ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var users []user
	for rows.Next() {
		var u user
		require.NoError(t, rows.Scan(&u.ID, &u.Name))
		users = append(users, u)
	}
	require.NoError(t, rows.Err())
	require.Len(t, users, 3)
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, "BaseUser", users[0].Name)
	require.Equal(t, 2, users[1].ID)
	require.Equal(t, "OverriddenUser", users[1].Name)
	require.Equal(t, 3, users[2].ID)
	require.Equal(t, "MainUser", users[2].Name)

	rows, err = db.Query("SELECT id, name, price FROM products ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	var products []product
	for rows.Next() {
		var p product
		require.NoError(t, rows.Scan(&p.ID, &p.Name, &p.Price))
		products = append(products, p)
	}
	require.NoError(t, rows.Err())
	require.Len(t, products, 3)
	require.Equal(t, 1, products[0].ID)
	require.Equal(t, "Milk", products[0].Name)
	require.InEpsilon(t, 2.50, products[0].Price, 0.0001)

	require.Equal(t, 2, products[1].ID)
	require.Equal(t, "Bread", products[1].Name)
	require.InEpsilon(t, 1.80, products[1].Price, 0.0001)

	require.Equal(t, 3, products[2].ID)
	require.Equal(t, "Phone", products[2].Name)
	require.InEpsilon(t, 399.99, products[2].Price, 0.0001)
}

func TestLoadPostgreSQL__complex_templates(t *testing.T) {
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase("db"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(10*time.Second),
		),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	time.Sleep(5 * time.Second)

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	cfg := &Config{
		FilePath:     "testdata/fixtures_templates_integration_complex.yml",
		ConnStr:      connStr,
		DatabaseType: PostgreSQL,
		Truncate:     true,
		ResetSeq:     true,
		DryRun:       false,
	}

	migrationSQL, err := os.ReadFile("testdata/migration_postgresql.sql")
	require.NoError(t, err, "read migrations")

	db, err := sql.Open("postgres", cfg.ConnStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(string(migrationSQL))
	require.NoError(t, err, "apply migrations")

	require.NoError(t, Load(context.Background(), cfg), "load fixtures")

	// Check users
	rows, err := db.Query("SELECT id, name, email, is_admin, super FROM users ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	type userCheck struct {
		ID      int
		Name    string
		Email   string
		IsAdmin sql.NullBool
		Super   sql.NullBool
	}
	var users []userCheck
	for rows.Next() {
		var u userCheck
		require.NoError(t, rows.Scan(&u.ID, &u.Name, &u.Email, &u.IsAdmin, &u.Super))
		users = append(users, u)
	}
	require.NoError(t, rows.Err())
	require.Len(t, users, 4)

	require.Equal(t, userCheck{ID: 1, Name: "Base User", Email: "user1@example.com"}, users[0])
	require.Equal(t, userCheck{ID: 2, Name: "Admin User", Email: "admin@example.com", IsAdmin: sql.NullBool{Bool: true, Valid: true}}, users[1])
	require.Equal(t, userCheck{ID: 3, Name: "Super Admin", Email: "superadmin@example.com", IsAdmin: sql.NullBool{Bool: true, Valid: true}, Super: sql.NullBool{Bool: true, Valid: true}}, users[2])
	require.Equal(t, userCheck{ID: 4, Name: "NoTemplate", Email: "notemplate@example.com"}, users[3])

	// Check products
	rows, err = db.Query("SELECT id, name, price FROM products ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()
	var products []product
	for rows.Next() {
		var p product
		require.NoError(t, rows.Scan(&p.ID, &p.Name, &p.Price))
		products = append(products, p)
	}
	require.NoError(t, rows.Err())
	require.Len(t, products, 3)
	require.Equal(t, product{ID: 1, Name: "Milk", Price: 2.50}, products[0])
	require.Equal(t, product{ID: 2, Name: "Bread", Price: 1.80}, products[1])
	require.Equal(t, product{ID: 3, Name: "Phone", Price: 399.99}, products[2])

	// Check orders
	rows, err = db.Query("SELECT id, user_id FROM orders ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()
	var orders []order
	for rows.Next() {
		var o order
		require.NoError(t, rows.Scan(&o.ID, &o.UserID))
		orders = append(orders, o)
	}
	require.NoError(t, rows.Err())
	require.Len(t, orders, 2)
	require.Equal(t, order{ID: 1, UserID: 1}, orders[0])
	require.Equal(t, order{ID: 2, UserID: 2}, orders[1])

	// Check orders2products
	rows, err = db.Query("SELECT order_id, product_id, quantity, price FROM orders2products ORDER BY order_id, product_id")
	require.NoError(t, err)
	defer rows.Close()
	var orderProducts []orderProduct
	for rows.Next() {
		var op orderProduct
		require.NoError(t, rows.Scan(&op.OrderID, &op.ProductID, &op.Quantity, &op.Price))
		orderProducts = append(orderProducts, op)
	}
	require.NoError(t, rows.Err())
	require.Len(t, orderProducts, 3)
	expectedOrderProducts := []orderProduct{
		{OrderID: 1, ProductID: 2, Quantity: 2, Price: 3.60},
		{OrderID: 2, ProductID: 1, Quantity: 1, Price: 2.50},
		{OrderID: 2, ProductID: 3, Quantity: 1, Price: 399.99},
	}
	for i, expected := range expectedOrderProducts {
		require.Equal(t, expected.OrderID, orderProducts[i].OrderID)
		require.Equal(t, expected.ProductID, orderProducts[i].ProductID)
		require.Equal(t, expected.Quantity, orderProducts[i].Quantity)
		require.InEpsilon(t, expected.Price, orderProducts[i].Price, 0.0001)
	}
}
