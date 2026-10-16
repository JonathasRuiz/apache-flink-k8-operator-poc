# Runbook — bare-metal PoC run order

This is the step-by-step runbook to deploy the Flink Operator PoC on your
bare-metal Kubernetes cluster and exercise the full ETL round-trip. Follow the
sections in order.

```
  [1] local Kafka      [2] cluster prereqs     [3] operator
      (compose)   -->      (local-path,    -->     (helm)
                            cert-manager)
                                                      |
  [6] consume  <--  [5] publish load  <--  [4] submit demo job
   (app.py)          (publisher_sim.py)       (application-job.yaml)
```

Prerequisites on your workstation: `docker` (+ compose), `kubectl` pointed at
your cluster, `helm`, and `python3` with `pip`.

---

## 0. Python deps for the local scripts

```bash
pip install kafka-python apache-flink
```

## 1. Start local Kafka

```bash
docker compose -f .local/compose.yml up -d
docker compose -f .local/compose.yml ps          # kafka healthy?
# Optional UI at http://localhost:8080
```

Verify the broker answers on the bootstrap address values-dev.yaml references:

```bash
docker exec poc-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## 2. Cluster prerequisites

Install the local-path provisioner and the checkpoint PVC
(see `deploy/flink/local-path.md` for detail):

```bash
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.30/deploy/local-path-storage.yaml
kubectl get storageclass                         # expect: local-path

kubectl create namespace flink
kubectl apply -f deploy/flink/checkpoint-pvc.yaml
kubectl -n flink get pvc flink-checkpoints       # Pending until first consumer = OK
```

Install cert-manager (operator webhook dependency — see `deploy/operator/install.md`):

```bash
helm repo add jetstack https://charts.jetstack.io && helm repo update
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true --version v1.16.2
kubectl -n cert-manager get pods                 # all 3 Running before continuing
```

## 3. Install the Flink Kubernetes Operator

```bash
helm repo add flink-operator-repo \
  https://downloads.apache.org/flink/flink-kubernetes-operator-1.10.0/
helm repo update
helm install flink-kubernetes-operator \
  flink-operator-repo/flink-kubernetes-operator \
  --namespace flink -f deploy/operator/values-dev.yaml

kubectl -n flink get pods                         # operator 2/2 Running
```

### >> Task 3.2 — verify the operator reconciles a FlinkDeployment

```bash
kubectl apply -f deploy/flink/session-cluster.yaml
kubectl -n flink get flinkdeployment flink-session-cluster -w
# PASS when status reaches STABLE and a session JobManager pod is Running.
```

This also satisfies task 5.2 (a session cluster accepting jobs). Keep it running
or delete it with `kubectl delete -f deploy/flink/session-cluster.yaml`.

## 4. Wire Kafka + submit the demo ETL (application mode)

```bash
kubectl apply -f deploy/flink/secret-kafka.yaml   # dev PLAINTEXT placeholder
```

Package `example_job.py` so the application-mode pod can run it. For the PoC the
simplest path is to bake it into a job image or mount it at
`/opt/flink/usrlib/example_job.py` (the path application-job.yaml references).
Then:

```bash
kubectl apply -f deploy/flink/application-job.yaml
kubectl -n flink get flinkdeployment etl-normalize -w
# PASS when a DEDICATED JobManager + TaskManager(s) for etl-normalize are Running.
```

> Networking note: the Flink pods must reach the Kafka bootstrap address in
> values-dev.yaml. On a single-node setup sharing the host network,
> `localhost:9092` works. On a multi-node cluster, change the bootstrap address
> (values-dev.yaml + the Secret) to the host IP or a Service that fronts the
> local broker, and re-apply.

### >> Task 5.3 — verify effective job configuration

```bash
# Open the Flink UI for the job (port-forward the JobManager REST port):
kubectl -n flink port-forward svc/etl-normalize-rest 8081:8081
# In the UI -> Job Manager -> Configuration, confirm:
#   pipeline.max-parallelism = 720
#   state.backend = rocksdb
#   state.checkpoints.dir = file:///flink-checkpoints
```

## 5. Publish synthetic load

```bash
# modest steady load
python .local/publisher_sim.py --rate 20 --count 0
```

## 6. Consume the transformed output

In another terminal:

```bash
python .local/app.py
# You should see normalized records: reading split into DOUBLE + unit,
# lower_snake field names, tenant/device/event_time present.
```

### >> Task 7.1 — end-to-end round-trip PASS

Records published to `ambyint-io-raw` appear transformed on
`ambyint-io-normalized` via `app.py`. That is the full publish -> transform ->
consume path working.

---

## Validation drills (tasks 7.2 - 7.4)

### >> Task 7.2 — checkpoint durability (recover after pod restart)

```bash
# delete a running TaskManager pod and watch the job recover from checkpoint
kubectl -n flink delete pod -l component=taskmanager,app=etl-normalize
kubectl -n flink get pods -w
# PASS: the job restarts and resumes from the latest checkpoint (no full reprocess
# from the beginning; offsets/state restored). Confirm continuity in app.py output.
```

### >> Task 7.3 — autoscaler rescales under load

```bash
# drive heavy, sustained load to create backpressure
python .local/publisher_sim.py --rate 500 --count 0

# watch the operator raise parallelism and redeploy from checkpoint
kubectl -n flink get flinkdeployment etl-normalize -w
kubectl -n flink get pods -w
# PASS: TaskManager count / job parallelism increases and SETTLES at or below the
# configured ceiling (job.autoscaler.vertex.max-parallelism = 12 in dev), with NO
# pods stuck permanently Pending. If pods pin Pending, lower the ceiling in
# application-job.yaml to fit your 3 nodes (see docs/promotion.md for sizing).
```

### >> Task 7.4 — confirm no HPA/KEDA on TaskManagers

```bash
kubectl -n flink get hpa
kubectl -n flink get scaledobjects.keda.sh 2>/dev/null
# PASS: neither targets the TaskManager pods. The operator autoscaler is the
# ONLY scaler. (Both commands returning nothing is the expected result.)
```

---

## Teardown

```bash
kubectl delete -f deploy/flink/application-job.yaml
kubectl delete -f deploy/flink/session-cluster.yaml
kubectl delete -f deploy/flink/secret-kafka.yaml
helm uninstall flink-kubernetes-operator -n flink
helm uninstall cert-manager -n cert-manager
docker compose -f .local/compose.yml down -v
```
