# Module 09: End-to-End Exactly-Once Processing (2PC Sink)

## 1. Two-Phase Commit Protocol
To achieve end-to-end exactly-once guarantees across streaming topologies:
1. **Pre-commit**: Flink coordinates with the external sink during checkpointing, creating a pending transaction.
2. **Commit**: When the JobManager confirms all operators successfully captured state, the sink commits the transaction.
