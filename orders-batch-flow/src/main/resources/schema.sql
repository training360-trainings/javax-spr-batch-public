create table if NOT EXISTS orders
(
    id            SERIAL PRIMARY KEY,
    customer_id     BIGINT,
    product_id     BIGINT,
    order_date     DATE,
    quantity    BIGINT,
    price    BIGINT,
    price_status VARCHAR(10)
);