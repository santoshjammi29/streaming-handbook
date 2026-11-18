# Module 02: Resilient Producers, Consumers & Cooperative Sticky Rebalance

## 1. Idempotent Producer Mechanics
Setting `enable.idempotence=true` guarantees exactly-once delivery within a single session:
- The broker assigns a unique Producer ID (PID).
- Each batch receives a monotonic Sequence Number per partition.
- The broker de-duplicates any retransmitted batches automatically.
