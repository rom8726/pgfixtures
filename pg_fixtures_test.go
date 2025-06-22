//go:build integration

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
				WithStartupTimeout(5*time.Second),
		),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

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
