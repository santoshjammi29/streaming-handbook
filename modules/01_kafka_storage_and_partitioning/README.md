# Module 01: Kafka Storage Internals, Log Segments & Zero-Copy I/O

## 1. Physical Storage Architecture
Kafka partitions are organized into sequential disk segments:
- `.log`: Raw binary messages written sequentially.
- `.index`: Sparse index mapping logical offsets to physical byte positions in `.log`.
- `.timeindex`: Sparse index mapping message timestamps to logical offsets.

```mermaid
graph TD
    subgraph TopicPartition["Partition 0 on Broker Disk"]
        Seg1["0000000000.log (Closed)"]
        Seg2["0000010000.log (Closed)"]
        Active["0000020000.log (Active Appends)"]
        Idx["0000020000.index (Sparse Index)"]
        TIdx["0000020000.timeindex"]
    end
    Client["Producer Append"] --> Active
    Active -.->|4KB threshold| Idx
```

## 2. Zero-Copy `sendfile()` Network Transfer
Kafka bypasses JVM application memory buffers during consumer reads. By leveraging Linux `sendfile()` system calls, data transfers directly from OS Page Cache to Network Socket buffers, minimizing CPU overhead and context switching.
