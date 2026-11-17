package controller

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/notinferred/cronradar-kubernetes/internal/cronradar"
)

type fakeAPI struct {
	mu        sync.Mutex
	syncs     []cronradar.SyncRequest
	reports   []cronradar.Event
	failFirst int
}

func (f *fakeAPI) Sync(_ context.Context, request cronradar.SyncRequest) (*cronradar.SyncResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncs = append(f.syncs, request)
	return &cronradar.SyncResponse{}, nil
}

func (f *fakeAPI) Report(_ context.Context, event cronradar.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failFirst > 0 {
		f.failFirst--
		return &cronradar.HTTPError{Status: http.StatusServiceUnavailable}
	}
	f.reports = append(f.reports, event)
	return nil
}

func (f *fakeAPI) snapshot() ([]cronradar.SyncRequest, []cronradar.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]cronradar.SyncRequest(nil), f.syncs...), append([]cronradar.Event(nil), f.reports...)
}

var now = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

func ownedJob(owner *batchv1.CronJob, name string, startedAgo time.Duration, condition batchv1.JobConditionType, finishedAgo time.Duration) *batchv1.Job {
	isController := true
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: owner.Namespace, Name: name, UID: types.UID("uid-" + name),
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "CronJob", Name: owner.Name, UID: owner.UID, Controller: &isController}},
		},
	}
	if startedAgo >= 0 {
		started := metav1.NewTime(now.Add(-startedAgo))
		job.Status.StartTime = &started
	}
	if condition != "" {
		job.Status.Conditions = []batchv1.JobCondition{{
			Type: condition, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(now.Add(-finishedAgo)),
			Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit",
		}}
	}
	return job
}

func failedPod(job *batchv1.Job, exitCode int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: job.Namespace, Name: job.Name + "-x1",
			Labels: map[string]string{"batch.kubernetes.io/controller-uid": string(job.UID)},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exitCode, Reason: "Error", Message: "secret output"}},
			}},
		},
	}
}

func startController(t *testing.T, api *fakeAPI, namespaces []string, objects ...runtime.Object) *fake.Clientset {
	t.Helper()
	kube := fake.NewClientset(objects...)
	controller := New(Config{
		Namespaces:          namespaces,
		Source:              "kubernetes",
		Version:             "test",
		ApplicationTimeZone: "UTC",
		SyncInterval:        time.Hour,
		SyncDebounce:        20 * time.Millisecond,
		RetryBaseDelay:      10 * time.Millisecond,
		Now:                 func() time.Time { return now },
	}, kube, api, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := controller.Run(ctx); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return kube
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within 5s")
}

func TestSyncsRegistryAndReportsJobLifecycle(t *testing.T) {
	backup := cronJob("prod", "backup", "0 2 * * *", nil)
	skipped := cronJob("prod", "skipped", "* * * * *", map[string]string{AnnotationSkip: "true"})
	completed := ownedJob(backup, "backup-1", time.Minute, batchv1.JobComplete, 0)
	failed := ownedJob(backup, "backup-2", time.Minute, batchv1.JobFailed, 0)
	running := ownedJob(backup, "backup-3", 0, "", 0)
	stale := ownedJob(backup, "backup-old", 2*time.Hour, batchv1.JobComplete, time.Hour)
	skippedRun := ownedJob(skipped, "skipped-1", 0, batchv1.JobComplete, 0)
	api := &fakeAPI{}

	startController(t, api, nil, backup, skipped, completed, failed, running, stale, skippedRun, failedPod(failed, 3))

	eventually(t, func() bool { _, reports := api.snapshot(); return len(reports) == 5 })
	syncs, reports := api.snapshot()
	if len(syncs) != 1 || syncs[0].Mode != "reconcile" || syncs[0].Source != "kubernetes" ||
		len(syncs[0].Monitors) != 1 || syncs[0].Monitors[0].Key != "prod-backup" {
		t.Fatalf("syncs = %+v", syncs)
	}

	byRun := map[string][]string{}
	for _, report := range reports {
		if report.MonitorKey != "prod-backup" || report.Schedule != "0 2 * * *" || report.GracePeriod != 60 {
			t.Fatalf("report = %+v", report)
		}
		byRun[report.RunID] = append(byRun[report.RunID], report.Kind)
		if report.Kind == "fail" {
			if !strings.Contains(report.Message, "Job backup-2 failed: BackoffLimitExceeded") ||
				!strings.Contains(report.Message, "container main exited with code 3 (Error)") {
				t.Fatalf("fail message = %q", report.Message)
			}
			if strings.Contains(report.Message, "secret output") {
				t.Fatal("container termination messages must not be sent")
			}
		}
	}
	if strings.Join(byRun["uid-backup-1"], ",") != "start,complete" ||
		strings.Join(byRun["uid-backup-2"], ",") != "start,fail" ||
		strings.Join(byRun["uid-backup-3"], ",") != "start" {
		t.Fatalf("reports by run = %v", byRun)
	}
	if byRun["uid-backup-old"] != nil || byRun["uid-skipped-1"] != nil {
		t.Fatalf("stale and skipped runs must not be reported: %v", byRun)
	}
}

func TestCronJobChangesResyncAndRemovalsReconcile(t *testing.T) {
	backup := cronJob("prod", "backup", "0 2 * * *", nil)
	api := &fakeAPI{}
	kube := startController(t, api, nil, backup)
	eventually(t, func() bool { syncs, _ := api.snapshot(); return len(syncs) == 1 })

	// A status-only change, as on every scheduled run, must not resync.
	updated := backup.DeepCopy()
	scheduled := metav1.NewTime(now)
	updated.Status.LastScheduleTime = &scheduled
	if _, err := kube.BatchV1().CronJobs("prod").UpdateStatus(context.Background(), updated, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if syncs, _ := api.snapshot(); len(syncs) != 1 {
		t.Fatalf("status change caused a sync: %+v", syncs)
	}

	if err := kube.BatchV1().CronJobs("prod").Delete(context.Background(), "backup", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { syncs, _ := api.snapshot(); return len(syncs) == 2 })
	syncs, _ := api.snapshot()
	if syncs[1].Mode != "reconcile" || len(syncs[1].Monitors) != 0 {
		t.Fatalf("a deleted CronJob must leave the reconciled registry: %+v", syncs[1])
	}
}

func TestNamespaceFilterAndRetry(t *testing.T) {
	watched := cronJob("prod", "backup", "0 2 * * *", nil)
	ignored := cronJob("dev", "backup", "0 2 * * *", nil)
	api := &fakeAPI{failFirst: 1}

	startController(t, api, []string{"prod"}, watched, ignored,
		ownedJob(watched, "prod-run", 0, "", 0), ownedJob(ignored, "dev-run", 0, "", 0))

	eventually(t, func() bool { _, reports := api.snapshot(); return len(reports) == 1 })
	syncs, reports := api.snapshot()
	if len(syncs[0].Monitors) != 1 || syncs[0].Monitors[0].Key != "prod-backup" {
		t.Fatalf("only the watched namespace is declared: %+v", syncs[0].Monitors)
	}
	if reports[0].RunID != "uid-prod-run" || reports[0].Kind != "start" {
		t.Fatalf("a transient failure is retried: %+v", reports)
	}
}
