# CronRadar for Kubernetes

Install one controller in your cluster and every Kubernetes CronJob is monitored by [CronRadar](https://cronradar.com): you hear about the runs that fail, run late, never start, or whose CronJob disappears. No sidecar, no change to your containers and no per-job setup.

The controller watches `batch/v1` CronJobs and Jobs with read-only access. It declares every CronJob to CronRadar with its schedule, and reports each Job a CronJob creates as a start, a completion or a failure.

## Install

With Helm 3.8 or later:

```sh
helm install cronradar-kubernetes oci://ghcr.io/notinferred/charts/cronradar-kubernetes \
  --version 0.1.0 --namespace cronradar --create-namespace
```

Or with kubectl:

```sh
kubectl apply -f https://github.com/notinferred/cronradar-kubernetes/releases/download/v0.1.0/cronradar-kubernetes.yaml
```

Both install the same thing: a one-replica Deployment in the `cronradar` namespace, its ServiceAccount and the RBAC below. The pod starts once the API key Secret from **Setup** exists.

## Setup

You need:

- Kubernetes 1.21 or later (`batch/v1` CronJobs). `spec.timeZone` needs 1.27.
- A CronRadar account and an application for this cluster. Copy its API key (it starts with `ck_app_`) from the application's settings. Use a separate CronRadar application and API key for production, staging, and development, and for each cluster.
- Outbound HTTPS from the controller pod to `https://cron.life`.

The API key is the only required configuration. Store it in a Secret named `cronradar-api-key` in the `cronradar` namespace, reading it from an environment variable so it never enters shell history:

```sh
kubectl create namespace cronradar
kubectl create secret generic cronradar-api-key --namespace cronradar \
  --from-literal=api-key="$CRONRADAR_API_KEY"
```

To use another Secret, set `apiKey.existingSecret` and `apiKey.key` in the chart values. The chart never creates the Secret, so the key stays out of Helm values and release history.

## Quickstart

With the Secret created, install and check that the registry synced:

```sh
helm install cronradar-kubernetes oci://ghcr.io/notinferred/charts/cronradar-kubernetes \
  --version 0.1.0 --namespace cronradar
kubectl logs --namespace cronradar deployment/cronradar-kubernetes | grep "registry synced"
```

Every CronJob in the cluster now appears in your CronRadar application as `<namespace>-<name>`, with its schedule, and each run reports its result.

## Annotations

Every CronJob is monitored by default. Annotations on the CronJob change how; none is required.

| Annotation                   | Value                     | Effect                                                                                     |
| ---------------------------- | ------------------------- | ------------------------------------------------------------------------------------------ |
| `cronradar.com/skip`         | `"true"`                  | Not monitored. A monitor synced before is paused, with its history kept.                   |
| `cronradar.com/monitor-key`  | a key such as `nightly-backup` | Replaces the `<namespace>-<name>` key. Use it to keep an existing monitor's history.  |
| `cronradar.com/grace-period` | seconds, at least `60`    | How long a run may be late, as a warning, before the monitor is critical. The default is 60. |

Opt a CronJob out:

```sh
kubectl annotate cronjob nightly-report --namespace reports cronradar.com/skip=true
```

In a manifest:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: nightly-backup
  namespace: prod
  annotations:
    cronradar.com/monitor-key: nightly-backup
    cronradar.com/grace-period: "600"
spec:
  schedule: "0 2 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
            - name: backup
              image: busybox:1.37
              command: ["sh", "-c", "date"]
```

A suspended CronJob (`spec.suspend: true`) is left out like a skipped one, so its monitor is paused instead of alerting for runs that are not meant to happen. Resuming it resumes the monitor.

Keys are lowercased, separators become single hyphens, other punctuation is dropped, and a key over 100 characters is shortened to 91 characters, a hyphen and an 8-character hash, as in every CronRadar SDK. If two CronJobs produce the same key, the first in namespace and name order keeps it and the other is logged and left unmonitored until you give it a `cronradar.com/monitor-key`.

## Lifecycle

You don't report runs yourself; the controller reads them from the cluster:

| Kubernetes                                        | CronRadar  |
| ------------------------------------------------- | ---------- |
| A Job owned by the CronJob gets `status.startTime` | `start`    |
| The Job's `Complete` condition becomes true        | `complete` |
| The Job's `Failed` condition becomes true          | `fail`     |

Each report carries the Job's UID as `run_id`, so concurrent runs pair correctly, and an `Idempotency-Key`, so a repeated report is not counted twice. A Job retries its pods up to `backoffLimit` before it fails, so CronRadar hears about a failure once, after the last retry.

A failure message names the Job's reason and the first failing container's exit code, for example `Job nightly-backup-29012345 failed: BackoffLimitExceeded: Job has reached the specified backoff limit; pod nightly-backup-29012345-x7k2p: container backup exited with code 1 (Error)`. Logs, arguments, environment variables and container termination messages are never sent.

A Job started with `kubectl create job --from=cronjob/<name>` is owned by the CronJob and is reported as a run of it.

## Reference

The image runs one command, `/cronradar-kubernetes`. Each flag has an environment variable, which the chart sets.

| Flag              | Environment variable       | Default      | Meaning                                                                                         |
| ----------------- | -------------------------- | ------------ | ----------------------------------------------------------------------------------------------- |
|                   | `CRONRADAR_API_KEY`        | (required)   | The application's API key. Environment only, so it never appears in the process list.           |
| `-namespaces`     | `CRONRADAR_NAMESPACES`     | all          | Comma-separated namespaces to watch.                                                            |
| `-sync-interval`  | `CRONRADAR_SYNC_INTERVAL`  | `5m`         | Full registry sync interval, at least `1m`. Changes also sync within about 10 seconds.          |
| `-source`         | `CRONRADAR_SOURCE`         | `kubernetes` | Sync source. Monitors removed from one source never pause another source's.                    |
| `-time-zone`      | `CRONRADAR_TIME_ZONE`      | `UTC`        | The CronRadar application's time zone, used only to warn about CronJobs scheduled in another.   |
| `-health-address` | `CRONRADAR_HEALTH_ADDRESS` | `:8080`      | Serves `/healthz` (process up) and `/readyz` (caches loaded). Empty disables it.               |
|                   | `CRONRADAR_BASE_URL`       | `https://cron.life` | Ingestion endpoint, for testing against a local CronRadar.                               |
| `-version`        |                            |              | Prints the version and exits.                                                                  |

Each sync declares, per CronJob: `key`, `name` (`<namespace>/<name>`), `schedule`, `gracePeriod`, and `metadata` with `namespace`, `cronJob` and, when set, `timeZone`. It is sent with `"mode": "reconcile"` and `"source": "kubernetes"`, so CronRadar pauses the monitors of CronJobs that were deleted, skipped or suspended. Schedules are sent as written, with Kubernetes' `?` turned into `*`; `@hourly`, `@daily`, `@weekly`, `@monthly` and `@yearly` are accepted, and `@every` is rejected.

### RBAC

The controller only reads. The chart grants exactly:

| API group | Resources           | Verbs           | Why                                          |
| --------- | ------------------- | --------------- | -------------------------------------------- |
| `batch`   | `cronjobs`, `jobs`  | `list`, `watch` | Discover CronJobs and follow their Jobs      |
| `""`      | `pods`              | `list`          | Read a failed Job's container exit codes     |

With no namespaces set this is a ClusterRole. With `namespaces` set, the chart creates a Role and RoleBinding in each listed namespace and nothing cluster-wide. It never reads Secrets, ConfigMaps or pod logs, and never writes.

### Time zones

CronRadar evaluates every schedule in the application's time zone, set in the application's settings (UTC when unset). Kubernetes evaluates a CronJob in its `spec.timeZone`, or in kube-controller-manager's zone, which is UTC on almost every cluster. When the two differ, expected times are off by the offset and CronRadar alerts at the wrong time. Either:

- install a second controller with its own application and `source` for the CronJobs in that zone, and set that application's time zone to match; or
- rewrite the schedule in the application's zone.

The controller logs a warning for each CronJob whose zone differs from `timeZone`.

## Configuration

Helm values:

| Value                   | Default                                    | Meaning                                                    |
| ----------------------- | ------------------------------------------ | ---------------------------------------------------------- |
| `apiKey.existingSecret` | `cronradar-api-key`                        | Secret holding the API key                                 |
| `apiKey.key`            | `api-key`                                  | Key within that Secret                                     |
| `namespaces`            | `[]` (all)                                 | Namespaces to watch; a list also limits RBAC to them       |
| `syncInterval`          | `5m`                                       | Full sync interval                                         |
| `source`                | `kubernetes`                               | Sync source                                                |
| `timeZone`              | `UTC`                                      | The application's time zone, for warnings                  |
| `resources`             | requests 10m CPU, 32Mi; limit 128Mi memory | Container resources                                        |
| `image.repository`      | `ghcr.io/notinferred/cronradar-kubernetes` | Image                                                      |
| `image.tag`             | the chart's `appVersion`                   | Image tag                                                  |
| `image.digest`          | empty                                      | A `sha256:` digest pins the image and wins over the tag    |

`nodeSelector`, `tolerations`, `affinity`, `priorityClassName`, `imagePullSecrets`, `podAnnotations` and `podLabels` are passed through. Watch two namespaces only:

```sh
helm upgrade --install cronradar-kubernetes oci://ghcr.io/notinferred/charts/cronradar-kubernetes \
  --version 0.1.0 --namespace cronradar --set 'namespaces={prod,billing}'
```

Several clusters can share one application only if each sets its own `source`, for example `--set source=kubernetes-eu-1`; otherwise each cluster's sync pauses the other's monitors. A separate application per cluster is simpler.

The Deployment runs one replica with the `Recreate` strategy, because two replicas would report every run twice. Memory grows with the number of CronJobs and Jobs kept in the cluster; raise the limit if the pod is OOMKilled.

## Error handling

Monitoring never touches your workloads. The controller has no write access, adds nothing to your pods, and your CronJobs run the same whether it is running, failing or uninstalled.

- Every request to CronRadar times out after 5 seconds.
- A failed sync is logged and sent again on the next change or interval.
- A failed run report is retried up to 5 times with backoff, from 5 seconds up to 2 minutes, on network errors, `429` and `5xx`. Other `4xx` responses are logged and not retried.
- Reports are paced to one a second, with bursts of 10, to stay within the API's 60 requests a minute per key. The hourly limit is 1,000 requests, two per run, so an application can take about 500 runs an hour; split busier clusters across applications.
- If CronRadar rejects one CronJob's schedule, the other CronJobs still sync, but CronRadar pauses nothing in that request until the schedule is fixed or skipped.
- Events from while the controller was down are reported only if they happened in the last 10 minutes; older ones would record a stale run as current. CronRadar's missed-run detection covers the gap.
- More than 1,000 CronJobs are synced in batches without reconciliation, so deleted CronJobs are not paused; the controller logs this.
- A missing API key or an interval under 1 minute stops the controller at start with a message.

## Troubleshooting

Read the controller's log first; every sync and report is logged without the API key:

```sh
kubectl logs --namespace cronradar deployment/cronradar-kubernetes --tail=100
```

| Symptom                                                      | Cause and fix                                                                                                                           |
| ------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| Pod in `CreateContainerConfigError`                          | The Secret is missing. Create `cronradar-api-key` in `cronradar` as in **Setup**.                                                      |
| `CRONRADAR_API_KEY is not set`                               | The Secret exists but the key name differs; set `apiKey.key`.                                                                           |
| Stuck on `waiting for CronJob and Job caches`, pod not ready | RBAC is missing. Check with the command below.                                                                                          |
| `registry sync failed` with `HTTP 401`                       | The key is wrong or revoked. Rotate it in the application's settings and update the Secret.                                             |
| `monitor not synced` with a schedule reason                  | CronRadar rejected that schedule, such as `@every 1h`. Rewrite it as five fields or add `cronradar.com/skip: "true"`.                     |
| `HTTP 402` or `created_plan_limited`                         | The plan's monitor limit or budget is reached; the extra CronJobs stay paused. Skip some, or change plan.                              |
| `schedule runs in America/New_York but ...`                  | See **Time zones**.                                                                                                                     |
| `monitor key ... is already used by`                         | Two CronJobs normalize to one key; give one a `cronradar.com/monitor-key`.                                                              |
| `report failed; retrying` with `HTTP 429`                    | More runs than the rate limit allows; see **Error handling**.                                                                           |
| A run shows only a completion                                | The controller restarted during that run; the start was more than 10 minutes old.                                                       |

Check the controller's access:

```sh
kubectl auth can-i list cronjobs.batch --all-namespaces \
  --as=system:serviceaccount:cronradar:cronradar-kubernetes
kubectl auth can-i list pods --namespace prod \
  --as=system:serviceaccount:cronradar:cronradar-kubernetes
```

## Verification

1. **Authentication and discovery.** Ask CronRadar what it holds, using Basic authentication so the key stays out of URLs:

   ```sh
   curl --fail-with-body --silent --show-error --user "$CRONRADAR_API_KEY:" \
     "https://cron.life/api/v1/sync/status"
   ```

   Every monitored CronJob is listed by key with its schedule.

2. **A completed run.** Create a Job from a CronJob and watch it reported:

   ```sh
   kubectl create job --from=cronjob/nightly-backup cronradar-check --namespace prod
   kubectl wait --for=condition=complete job/cronradar-check --namespace prod --timeout=5m
   kubectl logs --namespace cronradar deployment/cronradar-kubernetes | grep "key=nightly-backup"
   ```

   The log shows `reported event=start` and `reported event=complete` with the Job's UID, and the monitor's last execution is `success` in the app. Remove the Job with `kubectl delete job cronradar-check --namespace prod`.

3. **A failed run.** Apply a CronJob that always fails, then run it:

   ```sh
   kubectl create cronjob cronradar-fail-check --namespace default --image=busybox:1.37 \
     --schedule="0 0 1 1 *" -- sh -c "exit 3"
   kubectl patch cronjob cronradar-fail-check --namespace default --type=merge \
     -p '{"spec":{"jobTemplate":{"spec":{"backoffLimit":0}}}}'
   kubectl create job --from=cronjob/cronradar-fail-check cronradar-fail-check-1 --namespace default
   ```

   Within seconds the monitor `default-cronradar-fail-check` is `critical` with the message `... container cronradar-fail-check exited with code 3 (Error)`, and an alert reaches your channels. Delete the CronJob afterwards with `kubectl delete cronjob cronradar-fail-check --namespace default`; the next sync pauses its monitor.

4. **Notification delivery.** Send a test alert from the application's Alerts tab in the [dashboard](https://app.cronradar.com) and confirm it arrives.

## Migration

From pings in your containers (`curl https://cron.life/...` around the command, or a CronRadar SDK in a job that only runs on Kubernetes):

1. Install the controller with `cronradar.com/monitor-key` set on each CronJob to the key its pings use now, so the monitor's history continues.
2. Wait for one run of each and check it is reported in the app.
3. Remove the pings from the containers. Until then each run is reported twice under one key, which is harmless.

From another tool's Kubernetes agent: run both in parallel for a few days, compare the alerts, then uninstall the other agent.

To undo, uninstall. The monitors stay in CronRadar with their history, but nothing reports to them any more, so they alert as missed runs: pause or delete them in the dashboard first.

```sh
helm uninstall cronradar-kubernetes --namespace cronradar
```

With kubectl, delete what was applied:

```sh
kubectl delete -f https://github.com/notinferred/cronradar-kubernetes/releases/download/v0.1.0/cronradar-kubernetes.yaml
```

Renaming a CronJob or moving it to another namespace changes its key: the old monitor is paused and a new one created. Set `cronradar.com/monitor-key` to keep the old one.

## Links

- CronRadar: [cronradar.com](https://cronradar.com)
- Documentation: [docs.cronradar.com](https://docs.cronradar.com)
- Ingestion API contract: [docs.cronradar.com/openapi.json](https://docs.cronradar.com/openapi.json)
- Cron schedule syntax: [docs.cronradar.com/schedules](https://docs.cronradar.com/schedules)
- Source and issues: [github.com/notinferred/cronradar-kubernetes](https://github.com/notinferred/cronradar-kubernetes)
- Image: `ghcr.io/notinferred/cronradar-kubernetes`; chart: `oci://ghcr.io/notinferred/charts/cronradar-kubernetes`
- License: [MIT](LICENSE)
