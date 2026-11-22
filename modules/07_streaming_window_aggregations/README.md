# Module 07: Streaming Window Topologies (Tumbling, Sliding, Session)

![Spark Structured Streaming State Store & Watermarking Mechanics](../../assets/whiteboards/spark_streaming_state_store.webp)
> *Architectural Blueprint: [Spark Structured Streaming State Store & Watermarking Mechanics](../../docs/whiteboard_diagrams/README.md#spark-streaming-state-store). View full multi-persona breakdown and engine mechanisms in the master index.*

## 1. Window Semantics
- **Tumbling**: Fixed-size, non-overlapping windows ($[0, 10), [10, 20)$).
- **Sliding**: Fixed-size, overlapping evaluation intervals ($[0, 10)$ evaluated every 2 minutes).
- **Session**: Dynamic windows defined by inactivity gaps between consecutive events.
