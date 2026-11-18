# Module 10: High-Throughput Stream-to-Lakehouse Sinks

## 1. Structured Streaming into Delta Lake / Apache Iceberg
Real-time ingestion pipelines bridge message queues to object storage:
- RocksDB state store provider prevents driver/executor heap OOM during streaming state tracking.
- Trigger `AvailableNow` vs `ProcessingTime` enables configurable latency-cost tradeoffs.
- Deletion vectors and compaction maintenance prevent small-file performance degradation.
