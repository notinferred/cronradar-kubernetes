package controller

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"

	"github.com/notinferred/cronradar-kubernetes/internal/cronradar"
)

const (
	AnnotationSkip        = "cronradar.com/skip"
	AnnotationMonitorKey  = "cronradar.com/monitor-key"
	AnnotationGracePeriod = "cronradar.com/grace-period"

	DefaultGracePeriod = 60
	minimumGracePeriod = 60
)

// monitorFor describes how a CronJob is monitored. monitored is false for a CronJob
// that opts out or is suspended; warnings explain annotations that were ignored.
func monitorFor(cronJob *batchv1.CronJob) (monitor cronradar.Monitor, monitored bool, warnings []string) {
	id := cronJob.Namespace + "/" + cronJob.Name
	annotations := cronJob.Annotations
	if strings.EqualFold(strings.TrimSpace(annotations[AnnotationSkip]), "true") {
		return monitor, false, nil
	}
	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		return monitor, false, nil
	}

	key := cronradar.NormalizeKey(cronJob.Namespace + "-" + cronJob.Name)
	if override, ok := annotations[AnnotationMonitorKey]; ok {
		if normalized := cronradar.NormalizeKey(override); normalized != "" {
			key = normalized
		} else {
			warnings = append(warnings, fmt.Sprintf("%s: annotation %s=%q has no letters or digits; using %q", id, AnnotationMonitorKey, override, key))
		}
	}

	grace := DefaultGracePeriod
	if value, ok := annotations[AnnotationGracePeriod]; ok {
		seconds, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || seconds < minimumGracePeriod {
			warnings = append(warnings, fmt.Sprintf("%s: annotation %s=%q must be a whole number of seconds, at least %d; using %d", id, AnnotationGracePeriod, value, minimumGracePeriod, grace))
		} else {
			grace = seconds
		}
	}

	monitor = cronradar.Monitor{
		Key:         key,
		Name:        id,
		Schedule:    normalizeSchedule(cronJob.Spec.Schedule),
		GracePeriod: grace,
		Metadata:    map[string]string{"namespace": cronJob.Namespace, "cronJob": cronJob.Name},
	}
	if cronJob.Spec.TimeZone != nil && *cronJob.Spec.TimeZone != "" {
		monitor.Metadata["timeZone"] = *cronJob.Spec.TimeZone
	}
	return monitor, true, warnings
}

// normalizeSchedule turns Kubernetes' "?" (any value) into "*", which CronRadar's parser expects.
// Everything else is sent as written; the server reports a schedule it rejects.
func normalizeSchedule(schedule string) string {
	fields := strings.Fields(schedule)
	for index, field := range fields {
		if field == "?" {
			fields[index] = "*"
		}
	}
	return strings.Join(fields, " ")
}

type registry struct {
	monitors []cronradar.Monitor
	warnings []string
}

// buildRegistry declares every monitored CronJob, in a stable order. applicationTimeZone is
// the CronRadar application's zone; a CronJob evaluated in another zone gets a warning.
func buildRegistry(cronJobs []*batchv1.CronJob, applicationTimeZone string) registry {
	sorted := append([]*batchv1.CronJob(nil), cronJobs...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Namespace != sorted[j].Namespace {
			return sorted[i].Namespace < sorted[j].Namespace
		}
		return sorted[i].Name < sorted[j].Name
	})

	var result registry
	owners := map[string]string{}
	for _, cronJob := range sorted {
		monitor, monitored, warnings := monitorFor(cronJob)
		result.warnings = append(result.warnings, warnings...)
		if !monitored {
			continue
		}
		if owner, taken := owners[monitor.Key]; taken {
			result.warnings = append(result.warnings, fmt.Sprintf("%s: monitor key %q is already used by %s; set the %s annotation to monitor it", monitor.Name, monitor.Key, owner, AnnotationMonitorKey))
			continue
		}
		owners[monitor.Key] = monitor.Name
		if zone := cronJobTimeZone(cronJob); !sameZone(zone, applicationTimeZone) {
			result.warnings = append(result.warnings, fmt.Sprintf("%s: schedule runs in %s but the CronRadar application evaluates it in %s; expected times will be off (see README, \"Time zones\")", monitor.Name, zone, applicationTimeZone))
		}
		result.monitors = append(result.monitors, monitor)
	}
	return result
}

// cronJobTimeZone is the zone Kubernetes evaluates the schedule in. Without spec.timeZone it is
// kube-controller-manager's local zone, which is UTC on almost every cluster.
func cronJobTimeZone(cronJob *batchv1.CronJob) string {
	if cronJob.Spec.TimeZone != nil && *cronJob.Spec.TimeZone != "" {
		return *cronJob.Spec.TimeZone
	}
	return "UTC"
}

func sameZone(a, b string) bool {
	utc := func(zone string) bool {
		switch zone {
		case "UTC", "Etc/UTC", "Etc/UCT", "UCT", "Etc/Universal", "Universal", "Etc/Zulu", "Zulu", "Etc/GMT", "GMT":
			return true
		}
		return false
	}
	return a == b || (utc(a) && utc(b))
}
