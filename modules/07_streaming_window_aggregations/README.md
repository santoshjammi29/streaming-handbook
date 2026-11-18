# Module 07: Streaming Window Topologies (Tumbling, Sliding, Session)

## 1. Window Semantics
- **Tumbling**: Fixed-size, non-overlapping windows ($[0, 10), [10, 20)$).
- **Sliding**: Fixed-size, overlapping evaluation intervals ($[0, 10)$ evaluated every 2 minutes).
- **Session**: Dynamic windows defined by inactivity gaps between consecutive events.
