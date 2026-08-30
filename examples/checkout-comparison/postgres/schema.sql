-- Postgres control schema for the checkout comparison.
--
-- Mirrors the RiffDB contracts: the same columns, the same foreign keys, and
-- the same 96 seeded products the RiffDB seeds create. `stock_on_hand` carries
-- an explicit CHECK because Postgres integers go negative silently, where the
-- contracts declare `u64` and reject underflow.

DROP TABLE IF EXISTS order_lines, reservations, orders, products CASCADE;

CREATE TABLE products (
    tenant_id     uuid          NOT NULL,
    product_id    uuid          NOT NULL,
    sku           text          NOT NULL,
    name          text          NOT NULL,
    unit_price    numeric(38,2) NOT NULL,
    stock_on_hand bigint        NOT NULL CHECK (stock_on_hand >= 0),
    revision      bigint        NOT NULL,
    PRIMARY KEY (tenant_id, product_id),
    UNIQUE (tenant_id, sku)
);

CREATE TABLE orders (
    tenant_id         uuid          NOT NULL,
    order_id          uuid          NOT NULL,
    customer_id       uuid          NOT NULL,
    status            text          NOT NULL,
    placed_at         timestamptz   NOT NULL,
    order_total       numeric(38,2) NOT NULL,
    payment_reference text,
    revision          bigint        NOT NULL,
    PRIMARY KEY (tenant_id, order_id)
);

CREATE TABLE order_lines (
    tenant_id  uuid          NOT NULL,
    order_id   uuid          NOT NULL,
    line_id    uuid          NOT NULL,
    product_id uuid          NOT NULL,
    quantity   bigint        NOT NULL,
    unit_price numeric(38,2) NOT NULL,
    line_total numeric(38,2) NOT NULL,
    PRIMARY KEY (tenant_id, order_id, line_id),
    FOREIGN KEY (tenant_id, order_id)
        REFERENCES orders (tenant_id, order_id) ON DELETE CASCADE
);

CREATE TABLE reservations (
    tenant_id      uuid        NOT NULL,
    product_id     uuid        NOT NULL,
    reservation_id uuid        NOT NULL,
    order_id       uuid        NOT NULL,
    quantity       bigint      NOT NULL,
    expires_at     timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, product_id, reservation_id),
    FOREIGN KEY (tenant_id, product_id)
        REFERENCES products (tenant_id, product_id)
);

-- The same 96 products, with the same identities, that `gen_seeds.py` writes.
INSERT INTO products (tenant_id, product_id, sku, name, unit_price, stock_on_hand, revision)
SELECT '018f5f7e-7b0c-7d9a-8f14-77f42f53c851'::uuid,
       ('018f0f8b-7c6d-7e31-8a4f-' || lpad(to_hex(i), 12, '0'))::uuid,
       'BENCH-' || i,
       'Bench product ' || i,
       19.99,
       100000000,
       1
FROM generate_series(0, 95) AS i;
