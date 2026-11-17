package controller

import (
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func cronJob(namespace, name, schedule string, annotations map[string]string) *batchv1.CronJob {
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Annotations: annotations, UID: types.UID("cj-" + name)},
		Spec:       batchv1.CronJobSpec{Schedule: schedule},
	}
}

func TestRegistryMonitorsEveryCronJobByDefault(t *testing.T) {
	registry := buildRegistry([]*batchv1.CronJob{
		cronJob("prod", "report", "@daily", nil),
		cronJob("prod", "Backup_DB", "0 2 * * ?", nil),
	}, "UTC")

	if len(registry.monitors) != 2 || len(registry.warnings) != 0 {
		t.Fatalf("registry = %+v", registry)
	}
	backup := registry.monitors[0]
	if backup.Key != "prod-backup-db" || backup.Name != "prod/Backup_DB" || backup.Schedule != "0 2 * * *" || backup.GracePeriod != DefaultGracePeriod {
		t.Fatalf("backup = %+v", backup)
	}
	if backup.Metadata["namespace"] != "prod" || backup.Metadata["cronJob"] != "Backup_DB" {
		t.Fatalf("metadata = %v", backup.Metadata)
	}
	if registry.monitors[1].Schedule != "@daily" {
		t.Fatalf("macros are sent as written, got %q", registry.monitors[1].Schedule)
	}
}

func TestRegistryHonoursAnnotationsAndSuspend(t *testing.T) {
	suspended := cronJob("prod", "suspended", "* * * * *", nil)
	yes := true
	suspended.Spec.Suspend = &yes

	registry := buildRegistry([]*batchv1.CronJob{
		cronJob("prod", "skipped", "* * * * *", map[string]string{AnnotationSkip: "TRUE"}),
		cronJob("prod", "not-skipped", "* * * * *", map[string]string{AnnotationSkip: "no"}),
		suspended,
		cronJob("prod", "renamed", "*/5 * * * *", map[string]string{AnnotationMonitorKey: "Billing.Invoices", AnnotationGracePeriod: "300"}),
		cronJob("prod", "bad-grace", "*/5 * * * *", map[string]string{AnnotationGracePeriod: "30"}),
	}, "UTC")

	keys := map[string]int{}
	for _, monitor := range registry.monitors {
		keys[monitor.Key] = monitor.GracePeriod
	}
	if len(keys) != 3 || keys["prod-not-skipped"] != 60 || keys["billing-invoices"] != 300 || keys["prod-bad-grace"] != 60 {
		t.Fatalf("monitors = %v", keys)
	}
	if len(registry.warnings) != 1 || !strings.Contains(registry.warnings[0], AnnotationGracePeriod) {
		t.Fatalf("warnings = %v", registry.warnings)
	}
}

func TestRegistrySkipsKeyCollisionsAndWarnsAboutTimeZones(t *testing.T) {
	newYork := "America/New_York"
	utc := "Etc/UTC"
	zoned := cronJob("a", "zoned", "0 9 * * *", nil)
	zoned.Spec.TimeZone = &newYork
	utcZoned := cronJob("a", "utc", "0 9 * * *", nil)
	utcZoned.Spec.TimeZone = &utc

	registry := buildRegistry([]*batchv1.CronJob{
		cronJob("a-b", "c", "* * * * *", nil),
		cronJob("a", "b-c", "* * * * *", nil),
		zoned,
		utcZoned,
	}, "UTC")

	if len(registry.monitors) != 3 {
		t.Fatalf("monitors = %+v", registry.monitors)
	}
	if registry.monitors[0].Name != "a/b-c" {
		t.Fatalf("the first CronJob in name order keeps the key, got %s", registry.monitors[0].Name)
	}
	joined := strings.Join(registry.warnings, "\n")
	if !strings.Contains(joined, "a-b/c: monitor key \"a-b-c\" is already used by a/b-c") ||
		!strings.Contains(joined, "a/zoned: schedule runs in America/New_York") || strings.Contains(joined, "a/utc") {
		t.Fatalf("warnings = %v", registry.warnings)
	}
	if registry.monitors[2].Metadata["timeZone"] != "America/New_York" {
		t.Fatalf("metadata = %v", registry.monitors[2].Metadata)
	}
}

func TestLongNamesAreShortenedToTheKeyLimit(t *testing.T) {
	registry := buildRegistry([]*batchv1.CronJob{
		cronJob(strings.Repeat("n", 63), strings.Repeat("j", 52), "* * * * *", nil),
	}, "UTC")
	if key := registry.monitors[0].Key; len(key) != 100 {
		t.Fatalf("key %q has length %d", key, len(key))
	}
}
