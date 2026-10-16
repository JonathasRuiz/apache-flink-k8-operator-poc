# Flink Kubernetes Operator — install (bare-metal)

The operator uses an admission webhook that needs TLS certificates issued by
**cert-manager**. So the order is: cert-manager first, then the operator. No
cloud dependency on bare-metal.

```
  cert-manager  -->  flink-kubernetes-operator  -->  FlinkDeployments
  (webhook TLS)      (reconciles CRDs)               (your jobs)
```

## 1. Install cert-manager (task 2.2)

```bash
helm repo add jetstack https://charts.jetstack.io
helm repo update

helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true \
  --version v1.16.2
```

Pin the version; check the [cert-manager releases](https://github.com/cert-manager/cert-manager/releases)
for the current stable tag.

### Verify cert-manager is Running BEFORE installing the operator

```bash
kubectl -n cert-manager get pods
# EXPECT all three Running:
#   cert-manager-xxxx
#   cert-manager-cainjector-xxxx
#   cert-manager-webhook-xxxx
```

Do not proceed until all three are `Running`. The operator install will fail its
webhook setup otherwise.

## 2. Install the Flink Kubernetes Operator (task 3.1)

```bash
# The operator chart is published to a versioned path. Pick a release from
# https://downloads.apache.org/flink/flink-kubernetes-operator-<version>/ and
# use its chart URL. Example with 1.10.0:
helm repo add flink-operator-repo \
  https://downloads.apache.org/flink/flink-kubernetes-operator-1.10.0/
helm repo update

kubectl create namespace flink   # if not already created

helm install flink-kubernetes-operator \
  flink-operator-repo/flink-kubernetes-operator \
  --namespace flink \
  -f deploy/operator/values-dev.yaml
```

### Verify the operator is Running and the webhook is served

```bash
kubectl -n flink get pods
# EXPECT:
#   flink-kubernetes-operator-xxxx   2/2 Running   (operator + webhook containers)

kubectl -n flink get validatingwebhookconfigurations | grep flink
# EXPECT: a flink-operator webhook configuration present
```

## 3. Smoke-test operator reconciliation (task 3.2)

Confirm the operator actually reconciles a FlinkDeployment before you submit the
real demo job. The simplest check is to apply the session cluster (no job jar
required) and watch it come up, then remove it:

```bash
kubectl apply -f deploy/flink/session-cluster.yaml
kubectl -n flink get flinkdeployment flink-session-cluster -w
# EXPECT: status progresses to JOBMANAGER_DEPLOYED / STABLE and a
#         flink-session-cluster JobManager pod reaches Running.

# tear the smoke test down again (optional — you may keep the session cluster):
# kubectl delete -f deploy/flink/session-cluster.yaml
```

If the FlinkDeployment is reconciled and the JobManager pod starts, the operator
is healthy and you can proceed to the demo job (see `.local/README.md`).

## Uninstall (cleanup)

```bash
helm uninstall flink-kubernetes-operator -n flink
helm uninstall cert-manager -n cert-manager
```
