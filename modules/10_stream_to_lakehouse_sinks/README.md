# Module 10: High-Throughput Stream-to-Lakehouse Sinks


![Stateful Streaming Failure Recovery & Dead-Letter Queue (DLQ) Remediation](../../assets/whiteboards/streaming_dlq_recovery_flow_architecture.webp)
> *Architectural Blueprint: [Stateful Streaming Failure Recovery & Dead-Letter Queue (DLQ) Remediation](../../docs/whiteboard_diagrams/README.md#streaming-dlq-recovery-flow). View full multi-persona breakdown and engine mechanisms in the master index.*



## 1. Structured Streaming into Delta Lake / Apache Iceberg
Real-time ingestion pipelines bridge message queues to object storage:
- RocksDB state store provider prevents driver/executor heap OOM during streaming state tracking.
- Trigger `AvailableNow` vs `ProcessingTime` enables configurable latency-cost tradeoffs.
- Deletion vectors and compaction maintenance prevent small-file performance degradation.
