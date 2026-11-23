-- OneShop's OLTP schema, plus a read-only login for the ETL jobs. Runs as the
-- RDS master user on every loadgen start, so every statement is idempotent.

CREATE TABLE IF NOT EXISTS users
(
    id         SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name  VARCHAR(100),
    email      VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS items
(
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(100),
    category   VARCHAR(100),
    price      DECIMAL(7, 2),
    inventory  INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS purchases
(
    id             SERIAL PRIMARY KEY,
    user_id        BIGINT REFERENCES users (id),
    item_id        BIGINT REFERENCES items (id),
    quantity       INT DEFAULT 1,
    purchase_price DECIMAL(12, 2),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- etluser can connect and SELECT, nothing else: a leaked ETL credential or a
-- buggy job cannot modify the source system.
DO
$$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'readonly') THEN
        CREATE ROLE readonly;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'etluser') THEN
        CREATE USER etluser WITH PASSWORD 'etlpassword';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE oneshop TO readonly;
GRANT USAGE ON SCHEMA public TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly;
GRANT readonly TO etluser;
