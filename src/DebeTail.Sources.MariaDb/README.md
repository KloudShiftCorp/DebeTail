# Notes

## Server prerequisites

Binary logging must be enabled and set to row-based format. Add the following to `my.cnf` (or `my.ini` on Windows) and restart the server:

```ini
[mysqld]
binlog_format         = ROW
binlog_row_image      = FULL
server-id             = 1          # required on MySQL 5.6/5.7 and MariaDB 10.1

# Optional: log full column metadata (column names) — MySQL 8.0.1+ / MariaDB 10.5+
binlog_row_metadata   = FULL

# Optional: disable automatic binlog expiry so old files are not purged
expire_logs_days      = 0
```

The replication user must have the `REPLICATION SLAVE` and `REPLICATION CLIENT` privileges:

```sql
CREATE USER 'repl_user'@'%' IDENTIFIED BY 'secret';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%';
FLUSH PRIVILEGES;
```

## Test setup

```sql
-- Create test tables
CREATE TABLE orders
(
    id         INT AUTO_INCREMENT PRIMARY KEY,
    customer   VARCHAR(100),
    total      DECIMAL(10, 2)
);

CREATE TABLE customers
(
    id   INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100)
);

-- Verify binary logging is active
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';

-- Check current binlog position (MySQL 8.2+)
SHOW BINARY LOG STATUS;

-- Check current binlog position (MariaDB / MySQL < 8.2)
SHOW MASTER STATUS;

-- Insert and trigger capture
INSERT INTO orders (customer, total) VALUES ('Alice', 99.99);
UPDATE orders SET total = 149.99 WHERE id = 1;
DELETE FROM orders WHERE id = 1;
```

## Disable / clean up

```sql
-- Verify no replicas are connected
SHOW SLAVE HOSTS;

-- Purge all binlog files (irreversible — disconnects any active replica)
RESET MASTER;
```

## Resume from a saved offset

The connector is pull-based — position tracking is the caller's responsibility. Persist the last `ChangeOffset.Value` from `AcknowledgeAsync` and pass it back on restart via `InitialOffset`:

```csharp
// Start from a saved checkpoint
options.InitialOffset = "mysql-bin.000008|195";

// Start from the current end of the binlog (capture only new changes)
options.InitialOffset = "";
```

The offset format is `"{binlog filename}|{position}"`, matching the values in `SHOW BINARY LOG STATUS` / `SHOW MASTER STATUS`.