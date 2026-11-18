# Module 05: Stateful Stream Processing & RocksDB Tuning

## 1. State Backends in Flink
- `HashMapStateBackend`: State stored on JVM heap. Fast access; vulnerable to GC pauses and memory limits.
- `EmbeddedRocksDBStateBackend`: State stored off-heap in out-of-core RocksDB key-value stores. Supports multi-terabyte state; scales beyond JVM heap limits.
