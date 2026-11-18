# 🚨 Production Runbook: Corrupt Event Deserialization & DLQ Routing

> **Severity**: `SEV-2`  
> **Domain**: Event Streaming & Real-Time Processing  
> **Key Focus**: Isolate malformed payloads, unparseable JSON/Avro, and route safely to dead-letter topics.

---

## 1. Emergency Decision Tree

```mermaid
flowchart TD
    Alert["🚨 Corrupt Event Deserialization & DLQ Routing (SEV-2)"] --> TriageCategory{"Identify Root Cause Category"}
    
    TriageCategory -->|"Configuration or Schema Drift"| ConfigTriage["Inspect Pipeline Schema & Type Contracts"]
    TriageCategory -->|"Resource Starvation or OOM"| ResourceTriage["Inspect Memory Pools, Off-Heap State & GC"]
    TriageCategory -->|"Network or Storage Timeout"| NetworkTriage["Inspect Downstream Latency & Deadlocks"]

    ConfigTriage --> FixConfig["Remediation: Pin Schema Contracts & Apply Reversion"]
    ResourceTriage --> FixResource["Remediation: Increase Worker Memory & Throttle Batch Slices"]
    NetworkTriage --> FixNetwork["Remediation: Trigger Failover & Backoff Retry Strategy"]
```

---

## 2. Immediate Diagnostic Procedures (First 5 Minutes)

### Step 1: Query Error Signatures and Pipeline Metrics
Run the following inspection commands to isolate the fault:
```bash
# Check pipeline task status and recent error traces
echo "Inspecting error status for pipeline..."
grep -E "ERROR|CRITICAL|Exception|Timeout" /var/log/pipeline.log | tail -n 25
```

### Step 2: Inspect Worker and State Health
Check memory, disk saturation, and open file descriptors:
```bash
# Verify worker container resource utilization
ps aux --sort=-%mem | head -n 10
```

---

## 3. Root Cause Analysis & Detailed Remediation

### Primary Root Cause
In production systems, this incident typically originates from sudden traffic surges, upstream breaking schema mutations, or resource limits being breached without backpressure.

### Remediation & Recovery Strategy:
1. **Apply Emergency Throttle**: Restrict incoming batch size or pause non-critical consumers.
2. **Apply Hotfix Configuration**:
   ```python
   # Emergency throttle configuration
   throttle_rate_per_sec = 5000
   retry_backoff_multiplier = 2.0
   max_retry_limit = 5
   ```
3. **Execute Recovery Step**: Re-run the validation test suite and verify consumer convergence.

---

## 4. Verification & Prevention Checklist

- [ ] Pipeline metrics return to baseline with 0 active error alerts.
- [ ] Processing latency conforms to standard production SLAs.
- [ ] Postmortem root cause documented and shared with architecture leads.
