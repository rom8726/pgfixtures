CREATE TABLE users (
    id INT NOT NULL AUTO_INCREMENT,
    name TEXT NOT NULL,
    email VARCHAR(255) UNIQUE,
    is_admin BOOLEAN,
    super BOOLEAN,
    last_login_at TIMESTAMP NULL,
    created_at TIMESTAMP,
    PRIMARY KEY (id)
);

CREATE TABLE orders (
    id INT NOT NULL AUTO_INCREMENT,
    user_id INT NOT NULL,
    created_at TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE products (
    id INT NOT NULL AUTO_INCREMENT,
    name TEXT NOT NULL,
    price DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    PRIMARY KEY (id)
);

CREATE TABLE orders2products (
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    price DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders2products_product_id ON orders2products(product_id);