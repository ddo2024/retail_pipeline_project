CREATE TABLE categories (
    category_id INT PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE brands (
    brand_id INT PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE suppliers (
    supplier_id INT PRIMARY KEY,
    name VARCHAR(100),
    contact_info VARCHAR(100)
);

CREATE TABLE loyalty_programs (
    loyalty_program_id INT PRIMARY KEY,
    name VARCHAR(100),
    points_per_dollar INT
);

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    loyalty_program_id INT,
    gender VARCHAR(10),
    age INT,
    created_at DATE,
    FOREIGN KEY (loyalty_program_id) REFERENCES loyalty_programs(loyalty_program_id)
);

CREATE TABLE stores (
    store_id INT PRIMARY KEY,
    name VARCHAR(100),
    location VARCHAR(100),
    manager_id INT
);

CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(100),
    role VARCHAR(50),
    store_id INT,
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    name VARCHAR(100),
    category_id INT,
    brand_id INT,
    supplier_id INT,
    price DECIMAL(10,2),
    created_at DATE,
    season VARCHAR(20),
    FOREIGN KEY (category_id) REFERENCES categories(category_id),
    FOREIGN KEY (brand_id) REFERENCES brands(brand_id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

CREATE TABLE payments (
    payment_id INT PRIMARY KEY,
    method VARCHAR(50),
    status VARCHAR(50),
    paid_at DATE
);

CREATE TABLE sales_transactions (
    transaction_id INT PRIMARY KEY,
    customer_id INT,
    store_id INT,
    employee_id INT,
    transaction_date DATE,
    total_amount DECIMAL(10,2),
    payment_id INT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
    FOREIGN KEY (payment_id) REFERENCES payments(payment_id)
);

CREATE TABLE sales_items (
    item_id INT PRIMARY KEY,
    transaction_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    discount DECIMAL(10,2),
    tax DECIMAL(10,2),
    FOREIGN KEY (transaction_id) REFERENCES sales_transactions(transaction_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE inventory (
    inventory_id INT PRIMARY KEY,
    store_id INT,
    product_id INT,
    quantity INT,
    last_updated DATE,
    FOREIGN KEY (store_id) REFERENCES stores(store_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE stock_movements (
    movement_id INT PRIMARY KEY,
    product_id INT,
    store_id INT,
    movement_type VARCHAR(20),
    quantity INT,
    movement_date DATE,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

CREATE TABLE purchase_orders (
    order_id INT PRIMARY KEY,
    supplier_id INT,
    order_date DATE,
    status VARCHAR(50),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

CREATE TABLE shipments (
    shipment_id INT PRIMARY KEY,
    order_id INT,
    store_id INT,
    shipped_date DATE,
    received_date DATE,
    FOREIGN KEY (order_id) REFERENCES purchase_orders(order_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

CREATE TABLE returns (
    return_id INT PRIMARY KEY,
    item_id INT,
    reason VARCHAR(100),
    return_date DATE,
    FOREIGN KEY (item_id) REFERENCES sales_items(item_id)
);

CREATE TABLE promotions (
    promotion_id INT PRIMARY KEY,
    name VARCHAR(100),
    start_date DATE,
    end_date DATE
);

CREATE TABLE campaigns (
    campaign_id INT PRIMARY KEY,
    name VARCHAR(100),
    budget DECIMAL(10,2),
    start_date DATE,
    end_date DATE
);

CREATE TABLE customer_feedback (
    feedback_id INT PRIMARY KEY,
    customer_id INT,
    store_id INT,
    product_id INT,
    rating VARCHAR(10),
    comments VARCHAR(255),
    feedback_date DATE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE store_visits (
    visit_id INT PRIMARY KEY,
    customer_id INT,
    store_id INT,
    visit_date DATE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

CREATE TABLE pricing_history (
    history_id INT PRIMARY KEY,
    product_id INT,
    price DECIMAL(10,2),
    effective_date DATE,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE discount_rules (
    rule_id INT PRIMARY KEY,
    product_id INT,
    discount_type VARCHAR(50),
    value DECIMAL(10,2),
    valid_from DATE,
    valid_to DATE,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE tax_rules (
    tax_id INT PRIMARY KEY,
    product_id INT,
    tax_rate VARCHAR(10),
    region VARCHAR(50),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);