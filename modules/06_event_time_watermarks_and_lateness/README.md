# Module 06: Event Time, Watermarking & Side Outputs


![Spark Structured Streaming State Store & Watermarking Mechanics](../../assets/whiteboards/spark_streaming_state_store.webp)
> *Architectural Blueprint: [Spark Structured Streaming State Store & Watermarking Mechanics](../../docs/whiteboard_diagrams/README.md#spark-streaming-state-store). View full multi-persona breakdown and engine mechanisms in the master index.*



## 1. Watermark Mechanics
A watermark $W(t)$ is a monotonically increasing assertion that no events with event timestamp $\le t$ will arrive in the future.
- **Bounded-out-of-orderness**: Allows a fixed duration delay (e.g., 30 seconds) for network latency.
- **Side Outputs**: Late-arriving events that fall behind the watermark are diverted to a Dead Letter Queue (DLQ) rather than silently discarded.
