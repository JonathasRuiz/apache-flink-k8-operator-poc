# Apache Flink Kubernetes Operator — PoC

Deploys the [Apache Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/)
and PyFlink ETL workloads that transform/normalize Kafka data into new Kafka
topics (near-real-time) and, later, sink into data warehouses (Snowflake,
StarRocks) for historical context.

This repository is designed to be promoted across three environments
(**dev / qa / prod**). Because this is a Proof of Concept, it targets a
**bare-metal Kubernetes cluster first**, with a fully self-contained local test
harness (local Kafka, a publisher simulator, a PyFlink job, and a consumer) that
requires **zero cloud dependencies**. Promotion to EKS is a per-environment
values + Secret swap — not a code change.

## Repository layout

```
.
├── README.md                     <- you are here
├── .local/                       <- self-contained local test harness
│   ├── README.md                 <- RUN ORDER / runbook (start here to deploy)
│   ├── compose.yml               <- local Kafka (PLAINTEXT, single broker) + UI
│   ├── publisher_sim.py          <- publishes synthetic records to the raw topic
│   ├── example_job.py            <- PyFlink ETL: raw -> transform/normalize -> out
│   └── app.py                    <- consumes the transformed output topic
├── deploy/
│   ├── operator/
│   │   ├── install.md            <- cert-manager + Flink operator Helm install
│   │   └── values-dev.yaml       <- operator Helm values (bare-metal)
│   ├── flink/
│   │   ├── local-path.md         <- local-path-provisioner install notes
│   │   ├── checkpoint-pvc.yaml   <- PVC for file:// durable checkpoints
│   │   ├── secret-kafka.yaml     <- dev Kafka connection Secret (PLAINTEXT)
│   │   ├── application-job.yaml  <- application-mode FlinkDeployment (demo ETL)
│   │   └── session-cluster.yaml  <- session-mode FlinkDeployment (small jobs)
│   └── values/
│       ├── values-dev.yaml       <- dev: local Kafka, file:// checkpoints
│       ├── values-qa.yaml        <- qa stub: Aiven + s3:// + IRSA (documented)
│       └── values-prod.yaml      <- prod stub: Aiven + s3:// + IRSA (documented)
└── docs/
    └── promotion.md              <- dev->qa/prod swap map + deferred open questions
```

## Environment model (dev / qa / prod)

The same manifests deploy to every environment. Only the per-env values and the
Kafka/credential Secret change:

| Concern          | dev (bare-metal PoC)      | qa / prod (EKS)                 |
| ---------------- | ------------------------- | ------------------------------- |
| Kafka bootstrap  | `localhost:9092` (local)  | Aiven host:port                 |
| Kafka security   | PLAINTEXT                 | SASL_SSL + mTLS                 |
| Checkpoint store | `file:///flink-checkpoints` (PVC) | `s3://.../flink-checkpoints` |
| Checkpoint auth  | none (PVC)                | IRSA / pod-identity             |
| Node scaling     | fixed 3 nodes             | cluster-autoscaler / Karpenter  |
| maxParallelism   | **720** (fixed forever)   | **720** (same — never changes)  |

See [`docs/promotion.md`](docs/promotion.md) for the full swap map.

## How to deploy

**Start with the runbook: [`.local/README.md`](.local/README.md).** It walks the
full run order for a bare-metal cluster — install prerequisites, install the
operator, submit the demo job, publish synthetic load, consume the output, and
observe the autoscaler rescaling.

## Scaling model (summary)

- **Scale OUT** by adding TaskManager pods + raising per-operator parallelism.
  The JobManager is coordinator-only (HA via standby, not throughput).
- The **Flink Operator's built-in autoscaler** is the only mechanism that scales
  TaskManagers. **No HPA/KEDA on TaskManagers.** On bare-metal it is bounded by
  an explicit parallelism ceiling because there is no node supplier to add nodes.
- State backend is **RocksDB** (embedded, per-TaskManager local disk) with
  **durable checkpoints** so the autoscaler can rescale from a checkpoint.
- `pipeline.max-parallelism = 720` is the **key-group count, fixed at first
  checkpoint**. It is set deliberately up front and never changed.
