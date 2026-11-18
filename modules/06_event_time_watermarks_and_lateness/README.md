# Module 06: Event Time, Watermarking & Side Outputs

## 1. Watermark Mechanics
A watermark $W(t)$ is a monotonically increasing assertion that no events with event timestamp $\le t$ will arrive in the future.
- **Bounded-out-of-orderness**: Allows a fixed duration delay (e.g., 30 seconds) for network latency.
- **Side Outputs**: Late-arriving events that fall behind the watermark are diverted to a Dead Letter Queue (DLQ) rather than silently discarded.
