# 🌊 The Real-Time Event Streaming & Ingestion Handbook

**Enterprise Reference Architecture, Production Incident Runbooks & 4-Tier Interview Preparation Matrix**

[![CI Validation](https://github.com/santoshjammi29/streaming-handbook/actions/workflows/ci.yml/badge.svg)](https://github.com/santoshjammi29/streaming-handbook/actions)
[![Domain](https://img.shields.io/badge/Domain-Event_Streaming_&_Real-Time_Processing-blue?style=flat-square)](#)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg?style=flat-square)](LICENSE)

> Production Apache Kafka, Apache Flink, Debezium CDC, and stream-to-lakehouse exactly-once architectures.  
> Part of the **Modern Data Engineering & Lakehouse Systems Encyclopedia** ecosystem anchored by [**moderndataengineer.uk**](https://moderndataengineer.uk).

[Platform Architecture](#️-platform-architecture) • [Whiteboard Blueprints](#-whiteboard-blueprints) • [Interactive CLI](#-interactive-cli-explorer) • [Architectures](#️-architectural-deep-dives) • [Runbooks](#-production-incident-runbooks) • [Interview Matrix](#-4-tier-interview-preparation-matrix)  • [Core Concepts (76 Topics)](#-core-curriculum-concepts--topics) • [Sister Repositories](#-ecosystem-interlinking--sister-repositories)

---

## 🧠 Core Curriculum Concepts & Topics (76 Topics Covered)

> 💡 **Enterprise Knowledge Base Integration**: This repository implements **76 production topics** from the [Modern Data Engineering Systems Encyclopedia (921 Topics)](https://moderndataengineer.uk/concepts).

### 📑 Topics Directory at a Glance

| Pillar / Module | Topic Count | Key Core Primitives Covered | Status |
|:---|:---:|:---|:---:|
| 🌊 **Real-Time Streaming Lakehouse, CDC & Table Formats** | **76** | Fabric Capacity Smoothing & Bursting 24-Hour Windows, CAP & PACELC Theorem in Modern Data Stores, Parquet Vectorized Decoding & Predicate Pushdown, Apache Iceberg Hidden Partitioning & Manifest List Architecture, and 72 more | ✅ Fully Documented |

<details open>
<summary><b>🌊 Real-Time Streaming Lakehouse, CDC & Table Formats (76 Topics)</b> — <i>Click to collapse/expand</i></summary>

| # | Topic / Concept | Level | Architectural Definition & Core Principle |
|:---:|---|:---:|---|
| 1 | **Fabric Capacity Smoothing & Bursting 24-Hour Windows** | 🟣<br>Architect | Resource management model amortizing heavy compute bursts over rolling multi-hour evaluation periods. |
| 2 | **CAP & PACELC Theorem in Modern Data Stores** | 🔴<br>Advanced | In a network partition (P), trade off Availability (A) vs Consistency (C). PACELC extends this: Else (E), trade off Latency (L) vs Consistency (C). |
| 3 | **Parquet Vectorized Decoding & Predicate Pushdown** | 🟢<br>Easy | SIMD-accelerated column decoding combined with metadata-driven row group skipping via min/max statistics. |
| 4 | **Apache Iceberg Hidden Partitioning & Manifest List Architecture** | 🟢<br>Easy | Decouples table partitioning from physical directory structures, avoiding Hive-style column path rewrites. |
| 5 | **Spark 4.0 AQE Dynamic Shuffle Coalescing & Skew Join Mitigation** | 🟡<br>Intermediate | Runtime query plan adjustments based on actual shuffle statistics collected during stage execution. |
| 6 | **Databricks Serverless Compute & Photon Vectorized C++ Engine** | 🟣<br>Architect | Instant-on, fully managed C++ query engine separating cluster provisioning from SQL execution. |
| 7 | **Structured Streaming Watermarking & Allowed Lateness** | 🔴<br>Advanced | Threshold specifying how far event-time can lag behind maximum observed event-time before data is dropped. |
| 8 | **Delta Lake Deletion Vectors (DV) vs Copy-on-Write (CoW)** | 🟡<br>Intermediate | Soft-delete bitmap index avoiding full Parquet file rewrites on UPDATE and DELETE operations. |
| 9 | **Data Mesh Federated Governance & Data Product Contracts (ODCS)** | 🟣<br>Architect | Decentralized domain ownership with centralized automated compliance and declarative schema contracts. |
| 10 | **Unity Catalog 3-Level Namespace & Attribute-Based Access Control (ABAC)** | 🔴<br>Advanced | Unified lakehouse governance with catalog.schema.table namespace and tag-based dynamic masking policies. |
| 11 | **Apache Airflow 3.0 Asset-Driven Scheduling & Deferrable Operators** | 🟡<br>Intermediate | Event-driven DAG execution triggered by dataset updates combined with async triggerer workers. |
| 12 | **FinOps DBU/Credit Allocation & Query Result Cache Reuse** | 🟣<br>Architect | System table telemetry tracking compute spend with multi-tier cache hits across SQL Warehouses. |
| 13 | **Databricks Asset Bundles (DABs) & Declarative DataOps Pipelines** | 🔴<br>Advanced | Infrastructure-as-Code for lakehouses defining jobs, pipelines, and permissions in version-controlled YAML. |
| 14 | **Model Context Protocol (MCP) & Lakehouse RAG Vector Search** | 🟣<br>Architect | Standardized protocol connecting LLMs to lakehouse metadata and managed vector search indexes. |
| 15 | **OpenLineage & Spark UI DAG Task Skew Diagnostics** | 🟡<br>Intermediate | Open-source standard for runtime lineage collection paired with Spark UI partition duration metrics. |
| 16 | **Power BI Direct Lake Mode: In-Memory VertiPaq Paging** | 🟣<br>Architect | Zero-copy semantic model serving directly from OneLake Delta Parquet without Import or DirectQuery latency. |
| 17 | **Cosmos DB Request Units (RU/s) & Analytical Store (HTAP)** | 🔴<br>Advanced | Hybrid Transactional/Analytical Processing isolating row-store OLTP from columnar analytical sync. |
| 18 | **ScyllaDB C++ Seastar Asynchronous Engine vs JVM Cassandra** | 🟣<br>Architect | Share-nothing, thread-per-core asynchronous C++ database architecture eliminating JVM GC latency. |
| 19 | **Vector Indexing Architecture: HNSW vs IVF in Enterprise RAG** | 🔴<br>Advanced | Trade-offs between graph-based Hierarchical Navigable Small World and inverted file cluster indexing. |
| 20 | **In-Process Analytics Revolution: DuckDB and Polars via Apache Arrow Zero-Copy** | 🟢<br>Easy | Serverless local analytical engines sharing columnar memory without serialization or network hops. |
| 21 | **Data Product Intake & Problem Framing** | 🟣<br>Architect | A formal engineering discovery gate that captures business problem statements, baseline metrics, downstream decision cycles, and platform risk tiering before technical design commences. |
| 22 | **Data Contract Specification & Drift Prevention** | 🟣<br>Architect | An explicit, version-controlled agreement between data producers and consumers specifying schema definitions, business semantics, SLA guarantees, and change management protocols. |
| 23 | **Architectural Decision Records (ADRs) for Data Platforms** | 🟣<br>Architect | A lightweight, immutable document capturing significant technical architecture decisions, the historical context, explored alternatives, and accepted long-term trade-offs. |
| 24 | **Zero-Downtime Expand/Contract Data Migration** | 🟣<br>Architect | A phased database and schema refactoring pattern that guarantees 100% continuous uptime and compatibility across active producers and consumers during major table schema evolutions. |
| 25 | **Supply Chain Levels for Software Artifacts (SLSA) in Data** | 🟣<br>Architect | A security framework of standards and verifiable build attestations that protect data pipeline packages, container images, and ML models from tampering throughout the software supply chain. |
| 26 | **Canary Data Pipeline Deployment & Abort Circuit** | 🟣<br>Architect | A release strategy where new pipeline code or transformation logic processes a small fraction of live data or runs in parallel shadow mode before full production cutover. |
| 27 | **Multi-Signal Data Observability & SLO Management** | 🟣<br>Architect | A comprehensive operational framework that continuously monitors the five pillars of data health (freshness, volume, schema, quality, and lineage) against defined Service Level Objectives (SLOs). |
| 28 | **Blameless Data Incident Postmortems & Cost of Downtime** | 🟣<br>Architect | A structured, blameless retrospective methodology for analyzing production data outages, identifying systemic root causes, quantifying financial impact, and implementing permanent safeguards. |
| 29 | **Data FinOps Unit Economics & Workload Right-Sizing** | 🟣<br>Architect | The practice of measuring and optimizing cloud data platform expenditures by attributing costs to granular business units, product features, and computational workloads. |
| 30 | **Dual-Region Lakehouse Disaster Recovery & RPO/RTO** | 🟣<br>Architect | An enterprise business continuity architecture featuring automated metadata synchronization, cross-region object storage replication, and rehearsed failover procedures. |
| 31 | **Data Asset Sunsetting & Graveyard Archiving** | 🟣<br>Architect | A formal decommission and archival lifecycle process that safely retires obsolete data assets, reclaims storage costs, and satisfies legal record retention compliance. |
| 32 | **Data SLA, SLO & Error Budget Specification** | 🟣<br>Architect | A quantifiable framework translating business expectations into measurable pipeline metrics: Service Level Indicators (SLIs), Service Level Objectives (SLOs), and enforceable Service Level Agreements (SLAs). |
| 33 | **Trunk-Based Development for Data Platforms** | 🟣<br>Architect | A source-control branching strategy where data engineers merge small, frequent commits into a single core branch (trunk/main), avoiding long-lived feature branches and merge hell. |
| 34 | **Devcontainers & Ephemeral Data Sandboxes** | 🟣<br>Architect | Containerized, reproducible local and cloud-based development environments that allow data engineers to spin up identical toolchains and isolated test databases in seconds. |
| 35 | **Idempotent Pipeline Patterns & Replay Safety** | 🟣<br>Architect | A pipeline architectural design ensuring that executing a data transformation multiple times with identical inputs produces the exact same output without duplicates or data corruption. |
| 36 | **Dead-Letter Queue (DLQ) Quarantine Architecture** | 🟣<br>Architect | An isolation mechanism that routes malformed, corrupted, or non-conforming data records to a quarantined storage queue without halting primary pipeline execution. |
| 37 | **DataOps Testing Pyramid & Quality Assertions** | 🟣<br>Architect | A structured testing hierarchy spanning unit tests, integration tests, contract verification, and production data quality assertions (Great Expectations, Soda). |
| 38 | **Column-Level Encryption & Tokenization (FPE)** | 🟣<br>Architect | Cryptographic data protection mechanisms that encrypt or tokenize sensitive PII attributes while preserving original data formats and enabling analytical queries. |
| 39 | **Policy-as-Code Guardrails for Data Infrastructure** | 🟣<br>Architect | The automated enforcement of organizational security, compliance, and FinOps policies through code-defined rules evaluated during CI/CD infrastructure deployments. |
| 40 | **Production Readiness Review (PRR) for Data Products** | 🟣<br>Architect | A structured architectural review checklist conducted before promoting a data product to production, verifying operational readiness across security, reliability, observability, and support. |
| 41 | **Data Incident Severity Matrix & Escalation Routing** | 🟣<br>Architect | A standardized taxonomy defining incident severity levels (SEV-1 to SEV-4) based on consumer impact, with automated paging and response time service levels. |
| 42 | **Delta Lake UniForm (Universal Format)** | 🟣<br>Architect | An open lakehouse capability that writes Delta Parquet data files once while generating Iceberg and Hudi metadata asynchronously for multi-engine zero-copy read interoperability. |
| 43 | **Apache Iceberg REST Catalog Specification** | 🟣<br>Architect | An open, vendor-neutral HTTP OpenAPI standard for metadata discovery, atomic commit coordination, and table credential vending across multi-engine lakehouses. |
| 44 | **Apache Polaris Catalog Federation** | 🟣<br>Architect | An open-source centralized metadata catalog built on the Apache Iceberg REST specification, providing unified cross-cloud governance and multi-engine interoperability. |
| 45 | **Apache Paimon Streaming Lakehouse** | 🔴<br>Advanced | A streaming lakehouse storage format developed for high-throughput changelog ingestion, sub-second real-time streaming updates, and primary-key upserts with Apache Flink. |
| 46 | **Delta Lake Column Mapping** | 🟡<br>Intermediate | A Delta Lake metadata feature that decouples logical column names exposed to users from physical column identifiers stored inside Parquet files. |
| 47 | **Iceberg Equality Deletes vs. Position Deletes** | 🔴<br>Advanced | The two deletion vector mechanisms supported by Apache Iceberg v2 to handle row-level deletions and updates without full data file rewrites. |
| 48 | **Parquet ZSTD Level 7 Recompression** | 🟡<br>Intermediate | A high-ratio columnar compression algorithm developed by Facebook, offering 35-40% smaller disk footprint than Snappy with virtually identical decompression speed. |
| 49 | **Delta Standalone Reader & Writer** | 🟡<br>Intermediate | A lightweight JVM library that allows reading and writing Delta Lake tables directly without requiring an active Apache Spark cluster or runtime. |
| 50 | **Iceberg Snapshot Expiration** | 🔴<br>Advanced | Purging historical Iceberg table metadata and unreferenced Parquet data files to reclaim cloud storage and bound manifest list scan latency. |
| 51 | **Delta Lake Conflict Detection Matrix** | 🟣<br>Architect | The formal concurrency matrix in Delta Lake defining which write operations (append, update, delete, compaction) can commit concurrently under WriteSerializable isolation. |
| 52 | **UniForm Iceberg Metadata Translator** | 🟣<br>Architect | The background asynchronous daemon in Delta Lake that converts Delta action commits into Apache Iceberg metadata manifest trees. |
| 53 | **Apache Iceberg Partition Evolution** | 🔴<br>Advanced | A table format capability that updates partitioning schemas (e.g. from month to day) without rewriting historical data or invalidating existing queries. |
| 54 | **Delta Lake Soft Deletion Vectors** | 🔴<br>Advanced | Binary vector files recording deleted row positions, decoupling mutations from 500MB Parquet rewrites. |
| 55 | **Apache Hudi Merge-On-Read (MOR)** | 🔴<br>Advanced | A table type storing real-time updates in compact columnar log files, merging with base Parquet files during query execution. |
| 56 | **Apache Hudi Copy-On-Write (COW)** | 🟡<br>Intermediate | A table type that rewrites entire Parquet data files upon receiving row updates, maximizing subsequent read scan speeds. |
| 57 | **Lakehouse UniForm Hudi Interoperability** | 🟣<br>Architect | Cross-engine interoperability standard converting Delta commit logs into Apache Hudi timeline commit files. |
| 58 | **Open Lakehouse Interoperability Contract** | 🟣<br>Architect | A tested agreement for how multiple engines discover, read, write, and govern shared table data and metadata. |
| 59 | **Federated Metadata Control Plane** | 🟣<br>Architect | A governance architecture that lets distributed catalogs and data products share discovery and policy context without requiring every dataset to move into one store. |
| 60 | **Cross-Cloud Egress-Aware Data Placement** | 🔴<br>Advanced | A placement decision that weighs compute location, data gravity, egress charges, latency, residency, and operational ownership across cloud boundaries. |
| 61 | **AI Data Workload Unit Economics** | 🔴<br>Advanced | The cost model for delivering one useful AI-enabled data task, including retrieval, query compute, model inference, storage, and human review. |
| 62 | **Copy-on-Write vs Merge-on-Read Table Format Trade-offs** | 🔴<br>Advanced | The fundamental write-latency versus read-amplification trade-off between rewriting entire data files on mutation versus appending delta deletion files. |
| 63 | **Apache Iceberg REST Catalog Open Specification** | 🟣<br>Architect | A vendor-neutral OpenAPI protocol that standardizes how query engines discover namespaces, load table metadata pointers, and perform atomic multi-table commit transactions. |
| 64 | **Lakehouse Deletion Vectors for Right-to-be-Forgotten Compliance** | 🔴<br>Advanced | The application of lightweight position deletion files to remove individual customer PII records from immutable Parquet tables in compliance with privacy regulations without rewriting historical files. |
| 65 | **Lakehouse Active-Passive Multi-Region Disaster Recovery** | 🟣<br>Architect | An enterprise disaster recovery pattern that replicates lakehouse table metadata and underlying object files to a secondary cloud region to meet strict RPO and RTO recovery mandates. |
| 66 | **FinOps Cost Attribution Matrix for Shared Catalogs** | 🟣<br>Architect | The allocation framework that attributes compute, storage, and egress spend back to specific business units and data products across shared enterprise lakehouse infrastructure. |
| 67 | **Hybrid Search Retrieval over Lakehouse Tables** | 🟡<br>Intermediate | An enterprise retrieval architecture that merges sparse keyword search (BM25) with dense vector embeddings to provide accurate, hallucination-resistant context for RAG agents. |
| 68 | **Delta Lake Deletion Vectors (Merge-on-Read)** | 🟣<br>Architect | A storage optimization that shifts Delta Lake write operations from Copy-on-Write (CoW) to Merge-on-Read (MoR) using compact RoaringBitmap row tombstones. |
| 69 | **Delta Lake Liquid Clustering & Space-Filling Curves** | 🟣<br>Architect | A dynamic layout optimization using Hilbert space-filling curves that replaces rigid Hive-style directory partitioning and manual Z-Ordering. |
| 70 | **Delta Lake Change Data Feed (CDC & table_changes TVF)** | 🟣<br>Architect | A native Delta table capability that exposes row-level change events (inserts, update pre-images, update post-images, deletes) between table versions. |
| 71 | **Clustered Columnstore Rowgroup Health** | 🟣<br>Architect | Columnstore rowgroup health measures the size, compaction state, and row count of columnar rowgroups, where the optimal target is exactly 1,048,576 rows per compressed rowgroup. |
| 72 | **QUALIFY Clause Window Filtering** | 🟡<br>Intermediate | The QUALIFY clause filters the results of window functions directly within a single SELECT statement without requiring nested subqueries or Common Table Expressions. |
| 73 | **The 11 Modern Database Paradigms** | 🟣<br>Architect | The 11 Database Paradigms classify storage systems by data structure, access patterns, and consistency guarantees to prevent anti-patterns in polyglot persistence. |
| 74 | **Star Schema Many-to-Many Bridge Tables** | 🔴<br>Advanced | A Bridge Table (junction table) is an intermediate modeling construct in a Kimball Star Schema that resolves many-to-many relationships between facts and dimensions without Cartesian fan-out. |
| 75 | **Cross-Engine Data Manipulation (Rosetta Stone)** | 🟡<br>Intermediate | The Cross-Engine Rosetta Stone is an architectural translation matrix mapping common data engineering operations across SQL, PySpark, Pandas, and Polars. |
| 76 | **Debezium CDC & Analytical Stream Decoupling** | 🟣<br>Architect | Debezium CDC is a distributed transaction log mining engine that captures row-level database changes in real time, streaming events into Kafka to decouple OLTP from analytical lakehouses. |

</details>

---

## 🎯 Overview & Mission

Modern enterprise data systems require rigorous operational governance, predictable failure recovery, deterministic schemas, and observable pipeline execution.

This repository provides **battle-tested production specifications**:
- **4 Deep-Dive Architectural Blueprints**: In-depth engine mechanics, state stores, and Mermaid diagrams.
- **6 Production Incident Disaster Recovery Runbooks**: SEV-1 and SEV-2 playbooks with Emergency Decision Trees.
- **4-Tier Progressive Interview Matrix & 50 Flashcards**: Level 100 to Level 400 questions and active recall deck.
- **52 Whiteboard Blueprints**: Architectural infographics mirrored under `assets/whiteboards/`.
- **Interactive Terminal Shell**: Instant search and study directly from the terminal via `python scripts/interactive_shell.py`.

---

## 🏛️ Platform Architecture

```mermaid
flowchart LR
    A["Raw Ingestion & Contracts"] --> B["Processing & State Machine"]
    B --> C["Validation & Quality Gates"]
    C --> D["Storage & Lakehouse ACID Sinks"]
    D --> E["Lineage, Governance & Telemetry"]
```

---

## 🖼️ Master Whiteboard Architecture Blueprints

The repository includes **52 publication-grade architectural whiteboard blueprints** mirrored under [`assets/whiteboards/`](assets/whiteboards) and cataloged with comprehensive multi-persona breakdowns, failure modes, and engine internals in [`docs/whiteboard_diagrams/README.md`](docs/whiteboard_diagrams/README.md).

### 📐 Visual Blueprints Showcase

<table>
  <tr>
    <td width="50%" valign="top">
      <a href="docs/whiteboard_diagrams/README.md#kafka-event-streaming-pipeline">
        <img src="assets/whiteboards/kafka_event_streaming_pipeline.webp" alt="Real-Time Event Streaming & Kafka Architecture" width="100%" />
      </a>
      <br><strong>Real-Time Event Streaming & Kafka Architecture</strong><br>
      <em>Distributed commit log, partition replication, consumer group rebalancing, and consumer lag.</em>
    </td>
    <td width="50%" valign="top">
      <a href="docs/whiteboard_diagrams/README.md#spark-streaming-state-store">
        <img src="assets/whiteboards/spark_streaming_state_store.webp" alt="Spark Structured Streaming State Store & Watermarking Mechanics" width="100%" />
      </a>
      <br><strong>Spark Structured Streaming State Store & Watermarking Mechanics</strong><br>
      <em>RocksDB state store, event-time watermarking, and incremental state commit checkpoints.</em>
    </td>
  </tr>
  <tr>
    <td width="50%" valign="top">
      <a href="docs/whiteboard_diagrams/README.md#cdc-debezium-lakehouse-pipeline">
        <img src="assets/whiteboards/cdc_debezium_lakehouse_pipeline.webp" alt="Real-Time Change Data Capture (CDC) Architecture" width="100%" />
      </a>
      <br><strong>Real-Time Change Data Capture (CDC) Architecture</strong><br>
      <em>Log-based CDC using Debezium, Kafka Connect, and streaming ingestion into Bronze Delta.</em>
    </td>
    <td width="50%" valign="top">
      <a href="docs/whiteboard_diagrams/README.md#streaming-dlq-recovery-flow">
        <img src="assets/whiteboards/streaming_dlq_recovery_flow_architecture.webp" alt="Stateful Streaming Failure Recovery & Dead-Letter Queue (DLQ) Remediation" width="100%" />
      </a>
      <br><strong>Stateful Streaming Failure Recovery & Dead-Letter Queue (DLQ) Remediation</strong><br>
      <em>Poison pill isolation, exponential backoff retries, and quarantine routing flows.</em>
    </td>
  </tr>
  <tr>
    <td width="50%" valign="top">
      <a href="docs/whiteboard_diagrams/README.md#lakehouse-ingestion-autoloader">
        <img src="assets/whiteboards/lakehouse_ingestion_autoloader_architecture.webp" alt="End-to-End Batch & Incremental Lakehouse Ingestion" width="100%" />
      </a>
      <br><strong>End-to-End Batch & Incremental Lakehouse Ingestion</strong><br>
      <em>Cloud storage file notification, schema evolution, and Auto Loader micro-batch ingestion.</em>
    </td>
    <td width="50%" valign="top">
      <a href="docs/whiteboard_diagrams/README.md#distributed-systems-resilience-architecture">
        <img src="assets/whiteboards/distributed_systems_resilience_architecture.webp" alt="Distributed Systems Design: High-Throughput & Resilient Architecture" width="100%" />
      </a>
      <br><strong>Distributed Systems Design: High-Throughput & Resilient Architecture</strong><br>
      <em>Circuit breaking, rate limiting, connection pooling, and resilient RPC topology.</em>
    </td>
  </tr>
  <tr>
    <td width="50%" valign="top">
      <a href="docs/whiteboard_diagrams/README.md#data-ingestion-strategy-matrix">
        <img src="assets/whiteboards/data_ingestion_strategy_matrix_architecture.webp" alt="Enterprise Data Movement & Replication Strategy Decision Flow" width="100%" />
      </a>
      <br><strong>Enterprise Data Movement & Replication Strategy Decision Flow</strong><br>
      <em>Decision tree for batch vs micro-batch vs continuous streaming replication.</em>
    </td>
    <td width="50%" valign="top">
      <a href="docs/whiteboard_diagrams/README.md#lakehouse-acid-concurrency">
        <img src="assets/whiteboards/lakehouse_acid_concurrency.webp" alt="Lakehouse ACID Concurrency & Write Conflict Resolution Matrix" width="100%" />
      </a>
      <br><strong>Lakehouse ACID Concurrency & Write Conflict Resolution Matrix</strong><br>
      <em>Two-phase commit, transactional sinks, and write conflict serialization.</em>
    </td>
  </tr>
</table>

> 🔍 **Explore all 52 blueprints with 5-persona breakdowns, failure modes, and engine internals in [docs/whiteboard_diagrams/README.md](docs/whiteboard_diagrams/README.md)**.

## 💻 Interactive CLI Explorer

Explore architectures, runbooks, and interview guides directly from your terminal:

```bash
# Browse catalog
python scripts/interactive_shell.py --list
python scripts/interactive_shell.py --runbooks
python scripts/interactive_shell.py --architectures
python scripts/interactive_shell.py --interview
python scripts/interactive_shell.py --ecosystem

# Run self-contained test suite
python scripts/run_tests.py
```

---

## 📦 Production Curriculum Modules

| Module | Title & Focus | Key Deliverables & Code |
| :---: | :--- | :--- |
| **01** | **[Kafka Storage Internals & Zero-Copy I/O](modules/01_kafka_storage_and_partitioning/README.md)** | Commit log segments (`.log`, `.index`, `.timeindex`), Linux `sendfile()` OS page cache, `server.properties` |
| **02** | **[Resilient Producers & Cooperative Consumers](modules/02_producers_consumers_and_consumer_groups/README.md)** | Idempotent producer (`acks=all`), sequence numbers, cooperative sticky rebalance, `producer_resilient.py` |
| **03** | **[Schema Registry & Binary Avro Serialization](modules/03_schema_registry_and_evolution/README.md)** | 5-byte wire framing, magic byte (`0x00`), backward/forward compatibility rules, `order_event.avsc` |
| **04** | **[Apache Flink Runtime & Chandy-Lamport](modules/04_flink_distributed_runtime/README.md)** | JobManager, TaskManager slots, streaming dataflow graphs, distributed barrier checkpointing |
| **05** | **[State Management & RocksDB Tuning](modules/05_state_management_and_rocksdb/README.md)** | ValueState, ListState, MapState, RocksDB off-heap memory allocation, state TTL, `stateful_dedup.py` |
| **06** | **[Event Time, Watermarks & Side Outputs](modules/06_event_time_watermarks_and_lateness/README.md)** | Bounded-out-of-orderness watermarks, late arriving data handling, dead-letter side outputs |
| **07** | **[Streaming Window Aggregations](modules/07_streaming_window_aggregations/README.md)** | Tumbling, sliding, session, and over windows; incremental aggregate vs full process window |
| **08** | **[CDC Log Mining & Debezium Connectors](modules/08_cdc_log_mining_and_debezium/README.md)** | Transactional log mining, Postgres `pgoutput` WAL parsing, MySQL binlogs, schema drift handling |
| **09** | **[End-to-End Exactly-Once Processing](modules/09_end_to_end_exactly_once/README.md)** | Two-Phase Commit Protocol (`TwoPhaseCommitSinkFunction`), idempotent transactional sinks |
| **10** | **[High-Throughput Stream-to-Lakehouse Sinks](modules/10_stream_to_lakehouse_sinks/README.md)** | PySpark Structured Streaming into Delta Lake / Iceberg, RocksDB state provider, `stream_to_delta.py` |

---

## 🏛️ Architectural Deep Dives

Four production-tested architectural specifications located under `architectures/`:

| Spec ID | Document Title | Target Level | Core Architectural Focus |
| :---: | :--- | :---: | :--- |
| **01** | **[Kafka Partition Storage & Consumer Group Protocol](architectures/01_kafka_storage_internals_and_consumer_groups.md)** | `Level 300-400` | Segment files, zero-copy index lookups, offset commit protocols, and rebalance algorithms. |
| **02** | **[Flink Distributed Runtime & Asynchronous Barrier Snapshots](architectures/02_flink_distributed_runtime_and_chandy_lamport.md)** | `Level 300-400` | Chandy-Lamport checkpointing, stream alignment, RocksDB state backends, and WAL sinks. |
| **03** | **[Change Data Capture (CDC) with Debezium & Kafka Connect](architectures/03_cdc_debezium_log_mining_architecture.md)** | `Level 300-400` | Database WAL mining (Postgres WAL, MySQL binlog), snapshotting, and tombstone event propagation. |
| **04** | **[Stream-to-Lakehouse Exactly-Once Ingestion](architectures/04_stream_to_lakehouse_exactly_once_mechanics.md)** | `Level 300-400` | Two-Phase Commit sinks, micro-batch coordination, and Delta/Iceberg atomic transaction logs. |

---

## 🚨 Production Incident Runbooks

Six operational disaster recovery runbooks located under `runbooks/`:

| Runbook ID | Title & Direct Link | Severity | Root Cause & Remediation Focus |
| :---: | :--- | :---: | :--- |
| **01** | **[Consumer Group Rebalance Storm & Lag Surge Triage](runbooks/01_consumer_group_rebalance_storm_triage.md)** | `SEV-1` | Triage max.poll.interval.ms expirations, heartbeats, and stop-the-world partition rebalances. |
| **02** | **[Flink Backpressure & Checkpoint Timeout Triage](runbooks/02_flink_backpressure_and_checkpoint_timeout_triage.md)** | `SEV-1` | Locate bottleneck operators, tune network buffer pools, and resolve unaligned checkpoints. |
| **03** | **[CDC Schema Drift & Database WAL Disk Exhaustion](runbooks/03_cdc_schema_drift_and_wal_exhaustion.md)** | `SEV-1` | Handle replication slot stalls, upstream disk 100% saturation, and schema evolution. |
| **04** | **[Corrupt Event Deserialization & DLQ Routing](runbooks/04_poison_pill_deserialization_dead_letter_queue.md)** | `SEV-2` | Isolate malformed payloads, unparseable JSON/Avro, and route safely to dead-letter topics. |
| **05** | **[Flink RocksDB Off-Heap Native Memory Leak Triage](runbooks/05_rocksdb_offheap_memory_leak_triage.md)** | `SEV-2` | Diagnose block cache over-allocation, write buffer spill, and container OS OOM kills. |
| **06** | **[Kafka Under-Replicated Partitions & Broker Failure Recovery](runbooks/06_kafka_under_replicated_partitions_disaster_recovery.md)** | `SEV-1` | Recover offline replicas, resolve leader election failures, and rebalance partition replicas. |

---

## 🎓 4-Tier Interview Preparation Matrix

Comprehensive technical interview guides and flashcard deck located under `interview/`:

| Tier | Role Target | Core Focus Areas | Direct Link |
| :---: | :--- | :--- | :---: |
| **Tier 1** | **Junior Streaming Engineer** | Kafka topics, partitions, producers, consumer offsets, basic serialization. | [`01_junior.md`](interview/01_junior_streaming_engineer.md) |
| **Tier 2** | **Mid-Level Streaming Engineer** | Consumer lag, Kafka Connect, Avro Schema Registry, Flink tumbling windows. | [`02_mid_level.md`](interview/02_mid_level_streaming_engineer.md) |
| **Tier 3** | **Senior Distributed Systems Engineer** | Chandy-Lamport state snapshots, RocksDB tuning, stream-stream joins, exact-once. | [`03_senior.md`](interview/03_senior_distributed_systems_engineer.md) |
| **Tier 4** | **Staff & Principal Streaming Architect** | Multi-datacenter Kafka replication (MirrorMaker 2), capacity math, edge IoT ingest. | [`04_staff.md`](interview/04_staff_principal_streaming_architect.md) |
| **Deck** | **50 Rapid-Fire Flashcards** | 50 active recall questions and answers for technical mastery. | [`05_flashcards.md`](interview/05_flashcards_deck_50.md) |

---

## 🌐 Ecosystem Interlinking & Sister Repositories

The Modern Data Engineering ecosystem is intentionally partitioned into specialized, tightly coupled reference repositories anchored by **[Modern Data Engineer Platform (`moderndataengineer.uk`)](https://github.com/santoshjammi29/moderndataengineer)**:

| Core Engineering Focus | Flagship Repository | Direct Deep-Link Topics & Implementations in Sister Repositories |
| :--- | :--- | :--- |
| **Apache Spark & PySpark Core** | **[The PySpark Handbook](https://github.com/santoshjammi29/pyspark-handbook)** | • **Delta Lake ACID & Liquid Clustering**: [Databricks Handbook Mod 03 & 04](https://github.com/santoshjammi29/databricks-handbook)<br>• **SQL Windowing Equivalents**: [SQL Handbook Mod 04](https://github.com/santoshjammi29/sql-handbook)<br>• **Azure VNet Injection & Fabric Spark**: [Azure Cheat Sheet](https://github.com/santoshjammi29/azure-cheat-sheet) |
| **Databricks Platform** | **[The Databricks Handbook](https://github.com/santoshjammi29/databricks-handbook)** | • **Core PyArrow UDFs & Higher-Order Functions**: [PySpark Handbook Mod 01 & 02](https://github.com/santoshjammi29/pyspark-handbook)<br>• **Databricks SQL vs ANSI Rosetta Stone**: [SQL Handbook Runbook 05](https://github.com/santoshjammi29/sql-handbook)<br>• **Azure Databricks Landing Zones & Bicep IaC**: [Azure Cheat Sheet](https://github.com/santoshjammi29/azure-cheat-sheet) |
| **Relational & Cloud MPP SQL** | **[The SQL Handbook](https://github.com/santoshjammi29/sql-handbook)** | • **Distributed Data Movement & Skew Salting**: [PySpark Runbook 03](https://github.com/santoshjammi29/pyspark-handbook)<br>• **Serverless SQL Warehouse Spill Tuning**: [Databricks Runbook 05](https://github.com/santoshjammi29/databricks-handbook)<br>• **Synapse & Fabric OneLake Direct Lake SQL**: [Azure Architecture 01](https://github.com/santoshjammi29/azure-cheat-sheet) |
| **Enterprise Cloud & Fabric** | **[Azure Enterprise Handbook](https://github.com/santoshjammi29/azure-cheat-sheet)** | • **Databricks Asset Bundles (DABs) & Lakeflow**: [Databricks Handbook Mod 07](https://github.com/santoshjammi29/databricks-handbook)<br>• **Structured Streaming & RocksDB State**: [PySpark Handbook Mod 06](https://github.com/santoshjammi29/pyspark-handbook)<br>• **Cloud Warehouse Cost Hardening**: [SQL Handbook Runbook 06](https://github.com/santoshjammi29/sql-handbook) |
| **Analytics Engineering & dbt** | **[The dbt Handbook](https://github.com/santoshjammi29/dbt-handbook)** | • **Incremental Merge Optimizations**: [SQL Handbook Mod 06](https://github.com/santoshjammi29/sql-handbook)<br>• **dbt on Databricks & Unity Catalog**: [Databricks Handbook Mod 07](https://github.com/santoshjammi29/databricks-handbook)<br>• **Automated WAP Quality Gates**: [DataOps Handbook](https://github.com/santoshjammi29/dataops-cicd-handbook) |
| **Real-Time Streaming** | **[The Streaming Handbook](https://github.com/santoshjammi29/streaming-handbook)** | • **PySpark Structured Streaming & Watermarks**: [PySpark Handbook Mod 06](https://github.com/santoshjammi29/pyspark-handbook)<br>• **Event Hubs & IoT Real-Time Engine**: [Azure Architecture 04](https://github.com/santoshjammi29/azure-cheat-sheet)<br>• **Debezium CDC to Delta Lake**: [Databricks Handbook Mod 04](https://github.com/santoshjammi29/databricks-handbook) |
| **Data Systems Design** | **[Data System Design Handbook](https://github.com/santoshjammi29/data-system-design-handbook)** | • **MPP Distributed Engine Internals**: [SQL Handbook Architecture 02](https://github.com/santoshjammi29/sql-handbook)<br>• **Unified Memory & Catalyst Engine**: [PySpark Architecture 01 & 02](https://github.com/santoshjammi29/pyspark-handbook)<br>• **Zero-Trust Enterprise Landing Zones**: [Azure Architecture 02](https://github.com/santoshjammi29/azure-cheat-sheet) |
| **DataOps & Platform CI/CD** | **[DataOps & Platform Handbook](https://github.com/santoshjammi29/dataops-cicd-handbook)** | • **Databricks Asset Bundles (DABs) IaC**: [Databricks Handbook Mod 07](https://github.com/santoshjammi29/databricks-handbook)<br>• **Azure Bicep IaC & GitHub Actions OIDC**: [Azure Cheatsheet](https://github.com/santoshjammi29/azure-cheat-sheet)<br>• **dbt Slim CI & State Deferral**: [dbt Handbook](https://github.com/santoshjammi29/dbt-handbook) |
| **Unified Web Platform** | **[Modern Data Engineer Hub](https://github.com/santoshjammi29/moderndataengineer)** | • **Interactive Simulators & 52 Blueprints**: [Live Platform (`moderndataengineer.uk`)](https://moderndataengineer.uk)<br>• **Google-Grade BM25F Search & Virtual DOM**: [Architecture Spec](https://github.com/santoshjammi29/moderndataengineer/blob/main/docs/STANDARDIZED_ARCHITECTURE.md) |


---

## 📜 License

Distributed under the **[MIT License](LICENSE)**. Maintained with ❤️ by [Santosh Jammi](https://github.com/santoshjammi29) (2026).
