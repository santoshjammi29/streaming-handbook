# Module 05: Stateful Stream Processing & RocksDB Tuning


![Spark Structured Streaming State Store & Watermarking Mechanics](../../assets/whiteboards/spark_streaming_state_store.webp)
> *Architectural Blueprint: [Spark Structured Streaming State Store & Watermarking Mechanics](../../docs/whiteboard_diagrams/README.md#spark-streaming-state-store). View full multi-persona breakdown and engine mechanisms in the master index.*



## 1. State Backends in Flink
- `HashMapStateBackend`: State stored on JVM heap. Fast access; vulnerable to GC pauses and memory limits.
- `EmbeddedRocksDBStateBackend`: State stored off-heap in out-of-core RocksDB key-value stores. Supports multi-terabyte state; scales beyond JVM heap limits.
