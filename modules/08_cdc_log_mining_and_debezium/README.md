# Module 08: CDC Log Mining & Debezium Connector Architecture

## 1. Transactional Log Mining
Debezium attaches to the database write-ahead log (WAL) without executing expensive SELECT queries:
- PostgreSQL: `pgoutput` plugin reads logical replication slots.
- MySQL: Connects as a replica and parses binlog row events.
- Emits envelope records containing `before`, `after`, and transaction metadata (`op: c, u, d`).
