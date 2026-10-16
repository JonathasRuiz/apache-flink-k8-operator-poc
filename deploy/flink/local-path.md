# local-path-provisioner (bare-metal checkpoint storage)

The Flink autoscaler rescales a job by restoring from its latest **durable**
checkpoint. On bare-metal we have no object store, so checkpoints go to a PVC
backed by Rancher's [local-path-provisioner](https://github.com/rancher/local-path-provisioner).
It is a single `kubectl apply`, no external dependency.

## Install

```bash
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.30/deploy/local-path-storage.yaml
```

Pin to a release tag (not `master`) so installs are reproducible. Check the
[releases page](https://github.com/rancher/local-path-provisioner/releases) for
the newest stable tag and update the version above as needed.

## Make it the default StorageClass (optional but convenient)

```bash
kubectl patch storageclass local-path \
  -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
```

## Verify (task 2.1 acceptance)

```bash
kubectl get storageclass
# EXPECT: a "local-path" class listed (marked (default) if you patched it)

kubectl -n local-path-storage get pods
# EXPECT: local-path-provisioner-xxxx  Running
```

## Create the checkpoint PVC

```bash
kubectl create namespace flink   # if not already created
kubectl apply -f deploy/flink/checkpoint-pvc.yaml

# Verify (task 2.3 acceptance):
kubectl -n flink get pvc flink-checkpoints
# NOTE: with the WaitForFirstConsumer binding mode used by local-path, the PVC
# stays "Pending" until the first pod mounts it. It binds when the Flink job
# (application-job.yaml) is scheduled. That is expected, not an error.
```

## Node-local limitation (important)

local-path volumes are `ReadWriteOnce` and node-local. The JobManager and the
TaskManagers that share this checkpoint directory must be scheduled onto the
**same node**. `deploy/flink/application-job.yaml` documents the nodeSelector /
affinity approach for the PoC. This limitation is exactly what object storage
(`s3://`) removes when you promote to EKS — see `docs/promotion.md`.
