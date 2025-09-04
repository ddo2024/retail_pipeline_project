
CREATE TABLE dim_customers (
    surrogate_key SERIAL PRIMARY KEY,
    customer_id INT,
    name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    loyalty_program_id INT,
    gender VARCHAR(10),
    age INT,
    created_at DATE,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    UNIQUE(customer_id, valid_from)
);

CREATE TABLE dim_products (
    surrogate_key SERIAL PRIMARY KEY,
    product_id INT,
    name VARCHAR(100),
    category_id INT,
    brand_id INT,
    supplier_id INT,
    price DECIMAL(10,2),
    created_at DATE,
    season VARCHAR(20),
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    UNIQUE(product_id, valid_from)
);

CREATE TABLE dim_employees (
    surrogate_key SERIAL PRIMARY KEY,
    employee_id INT,
    name VARCHAR(100),
    role VARCHAR(50),
    store_id INT,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    UNIQUE(employee_id, valid_from)
);

CREATE TABLE dim_stores (
    surrogate_key SERIAL PRIMARY KEY,
    store_id INT,
    name VARCHAR(100),
    location VARCHAR(100),
    manager_id INT,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    UNIQUE(store_id, valid_from)
);

CREATE TABLE dim_categories (
    surrogate_key SERIAL PRIMARY KEY,
    category_id INT,
    name VARCHAR(100),
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    UNIQUE(category_id, valid_from)
);

CREATE TABLE dim_brands (
    surrogate_key SERIAL PRIMARY KEY,
    brand_id INT,
    name VARCHAR(100),
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    UNIQUE(brand_id, valid_from)
);

CREATE TABLE dim_suppliers (
    surrogate_key SERIAL PRIMARY KEY,
    supplier_id INT,
    name VARCHAR(100),
    contact_info VARCHAR(100),
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    UNIQUE(supplier_id, valid_from)
);

CREATE TABLE dim_loyalty_programs (
    surrogate_key SERIAL PRIMARY KEY,
    loyalty_program_id INT,
    name VARCHAR(100),
    points_per_dollar INT,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    UNIQUE(loyalty_program_id, valid_from)
);



CREATE TABLE sales_transactions (
    transaction_id INT PRIMARY KEY,
    customer_id INT,
    store_id INT,
    employee_id INT,
    transaction_date DATE,
    total_amount DECIMAL(10,2),
    payment_id INT
);

CREATE TABLE sales_items (
    item_id INT PRIMARY KEY,
    transaction_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    discount DECIMAL(10,2),
    tax DECIMAL(10,2)
);

CREATE TABLE payments (
    payment_id INT PRIMARY KEY,
    method VARCHAR(50),
    status VARCHAR(50),
    paid_at DATE
);

CREATE TABLE inventory (
    inventory_id INT PRIMARY KEY,
    store_id INT,
    product_id INT,
    quantity INT,
    last_updated DATE
);

CREATE TABLE stock_movements (
    movement_id INT PRIMARY KEY,
    product_id INT,
    store_id INT,
    movement_type VARCHAR(20),
    quantity INT,
    movement_date DATE
);

CREATE TABLE purchase_orders (
    order_id INT PRIMARY KEY,
    supplier_id INT,
    order_date DATE,
    status VARCHAR(50)
);

CREATE TABLE shipments (
    shipment_id INT PRIMARY KEY,
    order_id INT,
    store_id INT,
    shipped_date DATE,
    received_date DATE
);

CREATE TABLE returns (
    return_id INT PRIMARY KEY,
    item_id INT,
    reason VARCHAR(100),
    return_date DATE
);

CREATE TABLE customer_feedback (
    feedback_id INT PRIMARY KEY,
    customer_id INT,
    store_id INT,
    product_id INT,
    rating VARCHAR(10),
    comments VARCHAR(255),
    feedback_date DATE
);

CREATE TABLE store_visits (
    visit_id INT PRIMARY KEY,
    customer_id INT,
    store_id INT,
    visit_date DATE
);

CREATE TABLE pricing_history (
    history_id INT PRIMARY KEY,
    product_id INT,
    price DECIMAL(10,2),
    effective_date DATE
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

CREATE TABLE discount_rules (
    rule_id INT PRIMARY KEY,
    product_id INT,
    discount_type VARCHAR(50),
    value DECIMAL(10,2),
    valid_from DATE,
    valid_to DATE
);

CREATE TABLE tax_rules (
    tax_id INT PRIMARY KEY,
    product_id INT,
    tax_rate VARCHAR(10),
    region VARCHAR(50)
);
