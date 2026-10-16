# Promotion: dev (bare-metal) -> qa / prod (EKS)

The same manifests under `deploy/` deploy to every environment. Promotion is a
**values + Secret swap, never a code change**. This document is the swap map and
the parking lot for deferred decisions.

## Swap map

| Concern            | dev (bare-metal)                 | qa / prod (EKS)                        | Where it changes                         |
| ------------------ | -------------------------------- | -------------------------------------- | ---------------------------------------- |
| Kafka bootstrap    | `localhost:9092`                 | Aiven `host:port`                      | `deploy/values/values-*.yaml` + Secret   |
| Kafka security     | PLAINTEXT                        | SASL_SSL (+ mTLS)                      | `deploy/flink/secret-kafka.yaml` contents|
| Checkpoint dir     | `file:///flink-checkpoints`      | `s3://.../flink-checkpoints`           | `deploy/values/values-*.yaml`            |
| Savepoint dir      | `file:///flink-savepoints`       | `s3://.../flink-savepoints`            | `deploy/values/values-*.yaml`            |
| Checkpoint auth    | none (PVC via local-path)        | IRSA / pod-identity (no static keys)   | ServiceAccount annotation on EKS         |
| Checkpoint volume  | PVC `flink-checkpoints` + nodeSel| none (object storage is shared)        | remove PVC mount + node pinning on EKS   |
| Node scaling       | fixed 3 nodes                    | cluster-autoscaler / Karpenter         | cluster add-on (platform layer)          |
| Autoscaler ceiling | low (fits 3 nodes), e.g. 12      | high (nodes elastic), e.g. 120–360     | `job.autoscaler.vertex.max-parallelism`  |
| maxParallelism     | **720**                          | **720** (identical, forever)           | NEVER changes                            |
| TM sizing          | ~1 cpu / 2Gi                     | larger (measure UDF footprint)         | `deploy/values/values-*.yaml`            |

### What stays byte-identical across all environments
- `example_job.py` (reads Kafka + topics from env; no addresses, no secrets).
- The FlinkDeployment manifest **structure** (`deploy/flink/*.yaml`).
- `pipeline.max-parallelism = 720` — the irreversible key-group count.

### The one swap that is larger than endpoint-only
Because dev uses a PVC (`file://`) and EKS uses object storage (`s3://`), the
checkpoint **path** changes, not just endpoint/creds. This is the accepted
trade-off of choosing a PVC over MinIO for the bare-metal PoC. On EKS also:
- drop the `flink-checkpoints` PVC volume/mount and the same-node pinning;
- grant S3 access via **IRSA** (annotate the `flink` ServiceAccount with the IAM
  role ARN) — no static keys in a Secret.

## Autoscaler ceiling sizing (bare-metal)

There is no node supplier on bare-metal, so the autoscaler must be bounded below
physical capacity or pods pin `Pending`.

```
  hardware ceiling  =  (TMs that fit on 3 nodes)  x  slots per TM
  logical ceiling   =  job.autoscaler.vertex.max-parallelism   <-- set BELOW hw

  TMs that fit = floor( (node_allocatable_mem - overhead) / TM_mem ) x 3 nodes
```

Measure `TM_mem` from the REAL PyFlink footprint (the Python UDF process runs
beside the JVM TM — budget for both via `taskmanager.memory.process.size`), then
set `vertex.max-parallelism` a notch below the hardware ceiling so scaling
settles visibly. dev starts at `12`; adjust to your measured capacity.

## Deferred open questions (for the EKS phase)

1. **Aiven topic partition strategy.** Source parallelism is capped by the
   partition count of the source topic; keyspace skew creates hot subtasks.
   Decide per-topic partition counts for `ambyint-io-*` before qa/prod so the
   autoscaler has real headroom to use. (Not needed for the local PoC — local
   topics auto-create.)
2. **Measured TM sizing + final autoscaler ceiling.** The dev numbers are
   placeholders. Measure the PyFlink UDF memory/CPU footprint on real nodes and
   set per-env sizing + ceiling from it.
3. **Snowflake / StarRocks sink design.** Out of scope for the bare-metal
   round-trip. Design later as a separate "warehouse-sink" workload class
   (checkpoint-heavy, batch-tolerant), distinct from the realtime Kafka->Kafka
   class the demo job represents.
4. **Chart layout reconciliation.** This repo uses sane defaults because the real
   `helm-charts/` / add-ons conventions were not available at authoring time.
   Reconcile naming/structure with a representative existing add-on when promoting
   into the gitops/helm layer.
