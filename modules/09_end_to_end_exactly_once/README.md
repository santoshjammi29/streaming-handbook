# Module 09: End-to-End Exactly-Once Processing (2PC Sink)


![Lakehouse ACID Concurrency & Write Conflict Resolution Matrix](../../assets/whiteboards/lakehouse_acid_concurrency.webp)
> *Architectural Blueprint: [Lakehouse ACID Concurrency & Write Conflict Resolution Matrix](../../docs/whiteboard_diagrams/README.md#lakehouse-acid-concurrency). View full multi-persona breakdown and engine mechanisms in the master index.*



## 1. Two-Phase Commit Protocol
To achieve end-to-end exactly-once guarantees across streaming topologies:
1. **Pre-commit**: Flink coordinates with the external sink during checkpointing, creating a pending transaction.
2. **Commit**: When the JobManager confirms all operators successfully captured state, the sink commits the transaction.
