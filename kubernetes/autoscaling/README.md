# Celery queue-depth autoscaling on GKE

This configuration is for the `ohn-devops-care` GKE Autopilot cluster:

- Worker: `care/care-backend-care-be-celery-worker`
- Queue: `celery`
- Replica range: 2–6
- Target: 20 waiting jobs per worker

The cluster currently runs GKE `1.34.9-gke.1322001` on the Stable channel, so
the Preview GKE `AutoscalingMetric` feature is not applicable. This setup uses
Google's Custom Metrics Stackdriver Adapter with Managed Service for Prometheus.

## Flow

```text
Redis queue
  -> care-metrics-exporter
  -> PodMonitoring (30-second scrape)
  -> Managed Service for Prometheus / Cloud Monitoring
  -> Custom Metrics Stackdriver Adapter
  -> external.metrics.k8s.io
  -> HPA
  -> Celery worker Deployment
```

### Why the adapter is required

The Kubernetes HPA controller does not query Prometheus or Cloud Monitoring
directly. It only reads Kubernetes metrics APIs:

- `metrics.k8s.io` for CPU and memory.
- `custom.metrics.k8s.io` for metrics attached to Kubernetes objects.
- `external.metrics.k8s.io` for metrics describing systems outside a
  Kubernetes object, such as a Redis queue.

Celery queue depth is an external metric. The Stackdriver Adapter registers
`external.metrics.k8s.io` and translates requests from that API into Cloud
Monitoring queries. The adapter runs in the `custom-metrics` namespace, but
this HPA does not use the `custom.metrics.k8s.io` API.

### How metric conversion works

The exporter exposes:

```text
celery_queue_length{queue="celery"} 100
```

GMP scrapes that endpoint and stores the series in Cloud Monitoring as:

```text
prometheus.googleapis.com/celery_queue_length/gauge
```

Kubernetes metric names cannot contain `/`, so the HPA uses `|`:

```text
prometheus.googleapis.com|celery_queue_length|gauge
```

The HPA sends that name and its selector to
`external.metrics.k8s.io`. The adapter:

1. Replaces `|` with `/` to recover the Cloud Monitoring metric type.
2. Converts `metric.labels.*` and `resource.labels.*` entries into a Cloud
   Monitoring time-series filter.
3. Reads the latest gauge values from Cloud Monitoring.
4. Applies `REDUCE_MAX` across matching exporter series.
5. Returns one Kubernetes `ExternalMetricValue` to the HPA.

Conceptually, the request becomes:

```text
metric.type =
  prometheus.googleapis.com/celery_queue_length/gauge

metric.labels.queue = celery
resource.labels.project_id = devops-care
resource.labels.location = asia-south1-a
resource.labels.cluster = ohn-devops-care
resource.labels.namespace = monitoring
resource.labels.job = care-metrics-exporter
reducer = REDUCE_MAX
```

`REDUCE_MAX` is important if multiple exporter Pods observe the same Redis
queue. Using `SUM` would count one queue repeatedly.

The adapter only retrieves and converts the metric. The HPA controller, not the
adapter, calculates and applies the replica count.

### How the HPA scales

`AverageValue: "20"` produces:

```text
desired replicas = ceil(queue depth / 20)
```

For a queue depth of 100, the queue metric requests five replicas. The HPA also
retains the existing CPU target and uses the larger recommendation.

Examples:

| Queue depth | Queue recommendation |
| ---: | ---: |
| 0–40 | 2, because `minReplicas` is 2 |
| 41–60 | 3 |
| 61–80 | 4 |
| 81–100 | 5 |
| 101 or more | 6, because `maxReplicas` is 6 |

## Files

- `../deployment.yaml`, `../service.yaml`, `../podmonitoring.yaml`: exporter
  resources. The dev cluster already runs these in `monitoring`.
- `hpa.yaml`: queue-depth plus CPU autoscaling.
- `hpa-cpu-only.yaml`: restores the previous CPU-only configuration.

External metrics are not implicitly restricted to the HPA namespace. The HPA
therefore filters the Cloud Monitoring series by project, location, cluster,
exporter namespace, job, and queue.

## 1. Install the adapter

Ensure no other adapter already owns `external.metrics.k8s.io`:

```bash
kubectl get apiservice v1beta1.external.metrics.k8s.io
```

Grant Monitoring read access to the adapter's GKE Workload Identity principal:

```bash
PROJECT_ID=devops-care
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" \
  --format='value(projectNumber)')"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --role=roles/monitoring.viewer \
  --member="principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/subject/ns/custom-metrics/sa/custom-metrics-stackdriver-adapter"
```

Install the official adapter manifest:

```bash
kubectl apply -f \
  https://raw.githubusercontent.com/GoogleCloudPlatform/k8s-stackdriver/cm-sd-adapter-v0.16.10/custom-metrics-stackdriver-adapter/deploy/production/adapter_new_resource_model.yaml

kubectl -n custom-metrics rollout status \
  deployment/custom-metrics-stackdriver-adapter
```

## 2. Verify the existing exporter

The dev cluster already runs `monitoring/care-metrics-exporter`. Verify it and
its `PodMonitoring`:

```bash
kubectl -n monitoring get \
  deployment/care-metrics-exporter \
  podmonitoring.monitoring.googleapis.com/care-metrics-exporter
```

If deploying into a different namespace or cluster, update every
`resource.labels.*` value in `hpa.yaml`.

## 3. Verify the metric API

```bash
kubectl get apiservice | grep metrics

kubectl get --raw \
  '/apis/external.metrics.k8s.io/v1beta1/namespaces/care/prometheus.googleapis.com%7Ccelery_queue_length%7Cgauge?labelSelector=metric.labels.queue%3Dcelery%2Cresource.labels.project_id%3Ddevops-care%2Cresource.labels.location%3Dasia-south1-a%2Cresource.labels.cluster%3Dohn-devops-care%2Cresource.labels.namespace%3Dmonitoring%2Cresource.labels.job%3Dcare-metrics-exporter%2Creducer%3DREDUCE_MAX' |
  jq .
```

The response must contain a value before creating the HPA.

The adapter accepts Cloud Monitoring's fully qualified keys, such as
`metric.labels.queue`; the short key `queue` fails with
`Metric label: "queue" is not allowed`.

## 4. Apply the HPA

The worker already has a Helm-managed CPU HPA. Apply `hpa.yaml` to update that
same object; do not create a second HPA for the Deployment.

```bash
kubectl apply -f kubernetes/autoscaling/hpa.yaml
kubectl -n care describe hpa care-backend-care-be-celery-worker
```

Move this configuration into the owning Helm chart after the dev test,
otherwise a future Helm release can restore the old CPU-only specification.

## 5. Confirm autoscaling

Watch the HPA, Deployment, and Pods:

```bash
kubectl -n care get hpa care-backend-care-be-celery-worker --watch
kubectl -n care get deployment care-backend-care-be-celery-worker --watch
kubectl -n care get pods \
  -l app.kubernetes.io/component=celery-worker --watch
```

Enqueue at least 100 valid, idempotent dev tasks through CARE or Celery. Do not
push arbitrary strings directly into Redis. Confirm:

- `AbleToScale=True` and `ScalingActive=True`.
- A `SuccessfulRescale` event appears in `kubectl describe hpa`.
- Approximately five replicas are requested while the sampled queue is 100.
- The Deployment eventually returns to two replicas after the configured
  15-minute scale-down stabilization period.

Queue depth excludes tasks already reserved or running in Celery workers, so
prefetch can make the sampled queue lower than the number originally submitted.

## Roll back

```bash
kubectl apply -f kubernetes/autoscaling/hpa-cpu-only.yaml
```

Official references:

- [GMP horizontal pod autoscaling](https://cloud.google.com/stackdriver/docs/managed-prometheus/hpa)
- [Custom Metrics Stackdriver Adapter](https://github.com/GoogleCloudPlatform/k8s-stackdriver/tree/master/custom-metrics-stackdriver-adapter)
