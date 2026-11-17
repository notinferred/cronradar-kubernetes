# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0]

### Added

- In-cluster controller that discovers every `batch/v1` CronJob, in all namespaces or the ones listed, and reconciles them to CronRadar on start, on every change (debounced) and every sync interval.
- Start, completion and failure reports for each Job a CronJob creates, read from Job status; failure messages name the Job's reason and the failing container's exit code.
- Annotations `cronradar.com/skip`, `cronradar.com/monitor-key` and `cronradar.com/grace-period`. Suspended and skipped CronJobs leave the registry, so their monitors are paused.
- Helm chart and plain manifests with read-only RBAC: `list` and `watch` on CronJobs and Jobs, `list` on pods.
- Distroless, non-root, multi-arch image for amd64 and arm64.
