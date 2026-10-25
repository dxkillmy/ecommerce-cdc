# E-commerce CDC Pipeline (WIP)

Real-time pipeline that captures changes from an OLTP Postgres database
with Debezium, streams them through Kafka, and loads them into a warehouse
modeled with dbt.

Postgres (OLTP) → Debezium → Kafka → Python consumer → Postgres (warehouse) → dbt

🚧 Work in progress