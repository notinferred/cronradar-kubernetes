// Package controller discovers CronJobs, keeps the CronRadar registry in sync and
// reports each Job a CronJob creates as a start, completion or failure.
package controller

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	batchlisters "k8s.io/client-go/listers/batch/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/notinferred/cronradar-kubernetes/internal/cronradar"
)

const (
	// A Job event older than this when first seen happened while the controller was not
	// running; reporting it now would record a stale run as current.
	maxEventAge = 10 * time.Minute
	maxRetries  = 5
	// Stays under the per-key telemetry limit of 60 requests a minute.
	reportsPerSecond = 1
	reportBurst      = 10
	maxLoggedErrors  = 20
)

// API is the part of the CronRadar client the controller uses.
type API interface {
	Sync(ctx context.Context, request cronradar.SyncRequest) (*cronradar.SyncResponse, error)
	Report(ctx context.Context, event cronradar.Event) error
}

type Config struct {
	// Namespaces to watch; empty watches every namespace.
	Namespaces          []string
	Source              string
	Version             string
	ApplicationTimeZone string
	SyncInterval        time.Duration
	// SyncDebounce gathers a burst of CronJob changes into one sync.
	SyncDebounce time.Duration
	// RetryBaseDelay is the first wait before a failed report is sent again; it doubles each retry.
	RetryBaseDelay time.Duration
	Now            func() time.Time
}

type runProgress struct {
	started  bool
	finished bool
}

type Controller struct {
	config    Config
	kube      kubernetes.Interface
	api       API
	log       *slog.Logger
	factories []informers.SharedInformerFactory
	cronJobs  map[string]batchlisters.CronJobLister
	trigger   chan struct{}
	events    workqueue.TypedRateLimitingInterface[cronradar.Event]
	limiter   *rate.Limiter
	ready     atomic.Bool

	mu         sync.Mutex
	runs       map[types.UID]*runProgress
	keyOwners  map[string]string
	lastSynced []cronradar.Monitor
}

func New(config Config, kube kubernetes.Interface, api API, log *slog.Logger) *Controller {
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.RetryBaseDelay <= 0 {
		config.RetryBaseDelay = 5 * time.Second
	}
	controller := &Controller{
		config:   config,
		kube:     kube,
		api:      api,
		log:      log,
		cronJobs: map[string]batchlisters.CronJobLister{},
		trigger:  make(chan struct{}, 1),
		events: workqueue.NewTypedRateLimitingQueue[cronradar.Event](
			workqueue.NewTypedItemExponentialFailureRateLimiter[cronradar.Event](config.RetryBaseDelay, 2*time.Minute)),
		limiter:   rate.NewLimiter(reportsPerSecond, reportBurst),
		runs:      map[types.UID]*runProgress{},
		keyOwners: map[string]string{},
	}
	namespaces := config.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{metav1.NamespaceAll}
	}
	for _, namespace := range namespaces {
		factory := informers.NewSharedInformerFactoryWithOptions(kube, 0, informers.WithNamespace(namespace))
		controller.factories = append(controller.factories, factory)
		controller.cronJobs[namespace] = factory.Batch().V1().CronJobs().Lister()
		factory.Batch().V1().Jobs().Informer()
	}
	return controller
}

// Ready reports whether the CronJob and Job caches have loaded.
func (c *Controller) Ready() bool { return c.ready.Load() }

// Run blocks until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) error {
	defer c.events.ShutDown()
	var synced []cache.InformerSynced
	for _, factory := range c.factories {
		factory.Start(ctx.Done())
		synced = append(synced,
			factory.Batch().V1().CronJobs().Informer().HasSynced,
			factory.Batch().V1().Jobs().Informer().HasSynced)
	}
	c.log.Info("waiting for CronJob and Job caches", "namespaces", c.scope())
	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
		return fmt.Errorf("caches did not load; check the RBAC rules for cronjobs and jobs")
	}

	c.syncRegistry(ctx, true)

	// Handlers added after the caches load still receive every existing object, and find its CronJob.
	for _, factory := range c.factories {
		if _, err := factory.Batch().V1().CronJobs().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(any) { c.requestSync() },
			UpdateFunc: c.cronJobUpdated,
			DeleteFunc: func(any) { c.requestSync() },
		}); err != nil {
			return err
		}
		if _, err := factory.Batch().V1().Jobs().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj any) { c.observeJob(ctx, obj) },
			UpdateFunc: func(_, obj any) { c.observeJob(ctx, obj) },
			DeleteFunc: func(obj any) { c.forgetJob(ctx, obj) },
		}); err != nil {
			return err
		}
	}
	c.ready.Store(true)
	c.log.Info("watching CronJobs", "namespaces", c.scope(), "source", c.config.Source)

	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		for c.sendNext(ctx) {
		}
	}()
	c.syncLoop(ctx)
	c.events.ShutDown()
	workers.Wait()
	return nil
}

func (c *Controller) scope() string {
	if len(c.config.Namespaces) == 0 {
		return "all"
	}
	return strings.Join(c.config.Namespaces, ",")
}

func (c *Controller) requestSync() {
	select {
	case c.trigger <- struct{}{}:
	default:
	}
}

// cronJobUpdated ignores status changes, such as lastScheduleTime on every run.
func (c *Controller) cronJobUpdated(old, updated any) {
	oldCronJob, ok1 := old.(*batchv1.CronJob)
	newCronJob, ok2 := updated.(*batchv1.CronJob)
	if ok1 && ok2 {
		oldMonitor, oldMonitored, _ := monitorFor(oldCronJob)
		newMonitor, newMonitored, _ := monitorFor(newCronJob)
		if oldMonitored == newMonitored && reflect.DeepEqual(oldMonitor, newMonitor) &&
			reflect.DeepEqual(oldCronJob.Spec.TimeZone, newCronJob.Spec.TimeZone) {
			return
		}
	}
	c.requestSync()
}

func (c *Controller) syncLoop(ctx context.Context) {
	ticker := time.NewTicker(c.config.SyncInterval)
	defer ticker.Stop()
	for {
		periodic := false
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			periodic = true
		case <-c.trigger:
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.config.SyncDebounce):
			}
			select {
			case <-c.trigger:
			default:
			}
		}
		c.syncRegistry(ctx, periodic)
	}
}

func (c *Controller) listCronJobs() []*batchv1.CronJob {
	var all []*batchv1.CronJob
	for _, lister := range c.cronJobs {
		cronJobs, err := lister.List(labels.Everything())
		if err != nil {
			c.log.Error("listing cached CronJobs failed", "error", err)
			continue
		}
		all = append(all, cronJobs...)
	}
	return all
}

// syncRegistry declares every monitored CronJob. A change-triggered sync is skipped when the
// registry matches the last one CronRadar accepted; the periodic sync always runs, so a lost
// request or a monitor edited elsewhere is reconciled again.
func (c *Controller) syncRegistry(ctx context.Context, force bool) {
	registry := buildRegistry(c.listCronJobs(), c.config.ApplicationTimeZone)
	owners := make(map[string]string, len(registry.monitors))
	for _, monitor := range registry.monitors {
		owners[monitor.Key] = monitor.Name
	}
	c.mu.Lock()
	c.keyOwners = owners
	unchanged := reflect.DeepEqual(c.lastSynced, registry.monitors) && c.lastSynced != nil
	c.mu.Unlock()
	for _, warning := range registry.warnings {
		c.log.Warn(warning)
	}
	if unchanged && !force {
		return
	}
	if len(registry.monitors) == 0 {
		c.log.Warn("no CronJobs to monitor; this sync pauses any monitors previously synced from this source", "namespaces", c.scope(), "source", c.config.Source)
	}

	mode := "reconcile"
	batches := [][]cronradar.Monitor{registry.monitors}
	if len(registry.monitors) > cronradar.MaxSyncMonitors {
		mode = "upsert"
		batches = nil
		for start := 0; start < len(registry.monitors); start += cronradar.MaxSyncMonitors {
			end := min(start+cronradar.MaxSyncMonitors, len(registry.monitors))
			batches = append(batches, registry.monitors[start:end])
		}
		c.log.Warn("more CronJobs than one sync request can carry; registering them in batches, so monitors of deleted CronJobs are not paused", "cronJobs", len(registry.monitors), "limit", cronradar.MaxSyncMonitors)
	}

	accepted := true
	for _, batch := range batches {
		response, err := c.api.Sync(ctx, cronradar.SyncRequest{
			Source:   c.config.Source,
			Version:  c.config.Version,
			Mode:     mode,
			Monitors: append([]cronradar.Monitor{}, batch...),
		})
		if err != nil {
			accepted = false
			c.log.Error("registry sync failed; it is retried on the next change or interval", "error", err)
			continue
		}
		summary := response.Summary
		c.log.Info("registry synced", "mode", mode, "monitors", len(batch), "created", summary.Created,
			"updated", summary.Updated, "unchanged", summary.Unchanged, "paused", summary.Removed, "errors", summary.Errors)
		for index, rejection := range response.Errors {
			if index == maxLoggedErrors {
				c.log.Error("further monitor errors omitted", "count", len(response.Errors)-maxLoggedErrors)
				break
			}
			c.log.Error("monitor not synced", "key", rejection.Key, "reason", rejection.Error)
		}
		if len(response.Errors) > 0 && mode == "reconcile" {
			c.log.Warn("CronRadar skipped removals because a monitor was rejected; monitors of deleted CronJobs stay active until it is fixed or skipped")
		}
	}
	if accepted {
		c.mu.Lock()
		c.lastSynced = registry.monitors
		if c.lastSynced == nil {
			c.lastSynced = []cronradar.Monitor{}
		}
		c.mu.Unlock()
	}
}

// monitorForJob finds the monitor of the CronJob that created job.
func (c *Controller) monitorForJob(job *batchv1.Job) (cronradar.Monitor, bool) {
	owner := metav1.GetControllerOf(job)
	if owner == nil || owner.Kind != "CronJob" {
		return cronradar.Monitor{}, false
	}
	lister, ok := c.cronJobs[job.Namespace]
	if !ok {
		lister = c.cronJobs[metav1.NamespaceAll]
	}
	if lister == nil {
		return cronradar.Monitor{}, false
	}
	cronJob, err := lister.CronJobs(job.Namespace).Get(owner.Name)
	if err != nil {
		return cronradar.Monitor{}, false
	}
	monitor, monitored, _ := monitorFor(cronJob)
	if !monitored {
		return cronradar.Monitor{}, false
	}
	c.mu.Lock()
	claimant, claimed := c.keyOwners[monitor.Key]
	c.mu.Unlock()
	if claimed && claimant != monitor.Name {
		return cronradar.Monitor{}, false
	}
	return monitor, true
}

func (c *Controller) observeJob(ctx context.Context, obj any) {
	job, ok := obj.(*batchv1.Job)
	if !ok {
		return
	}
	monitor, ok := c.monitorForJob(job)
	if !ok {
		return
	}
	now := c.config.Now()
	recent := func(at time.Time) bool { return now.Sub(at) <= maxEventAge }

	c.mu.Lock()
	progress := c.runs[job.UID]
	if progress == nil {
		progress = &runProgress{}
		c.runs[job.UID] = progress
	}
	sendStart := false
	if !progress.started && job.Status.StartTime != nil {
		progress.started = true
		sendStart = recent(job.Status.StartTime.Time)
	}
	var terminal *batchv1.JobCondition
	if !progress.finished {
		if terminal = terminalCondition(job); terminal != nil {
			progress.finished = true
			if !recent(terminal.LastTransitionTime.Time) {
				terminal = nil
			}
		}
	}
	c.mu.Unlock()

	event := cronradar.Event{
		MonitorKey:  monitor.Key,
		RunID:       string(job.UID),
		Schedule:    monitor.Schedule,
		GracePeriod: monitor.GracePeriod,
	}
	if sendStart {
		event.Kind = "start"
		c.events.Add(event)
	}
	if terminal == nil {
		return
	}
	if terminal.Type == batchv1.JobComplete {
		event.Kind = "complete"
	} else {
		event.Kind = "fail"
		event.Message = c.failureMessage(ctx, job, terminal)
	}
	c.events.Add(event)
}

// forgetJob first observes the Job's final state: a Job with ttlSecondsAfterFinished: 0 can
// be deleted before its completion reaches the controller as an update.
func (c *Controller) forgetJob(ctx context.Context, obj any) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	if job, ok := obj.(*batchv1.Job); ok {
		c.observeJob(ctx, job)
		c.mu.Lock()
		delete(c.runs, job.UID)
		c.mu.Unlock()
	}
}

func terminalCondition(job *batchv1.Job) *batchv1.JobCondition {
	for index := range job.Status.Conditions {
		condition := &job.Status.Conditions[index]
		if condition.Status == corev1.ConditionTrue &&
			(condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed) {
			return condition
		}
	}
	return nil
}

// failureMessage names the Job's failure reason and, when a pod shows one, the container exit
// code. Container termination messages are left out: they hold whatever the job wrote.
func (c *Controller) failureMessage(ctx context.Context, job *batchv1.Job, condition *batchv1.JobCondition) string {
	message := fmt.Sprintf("Job %s failed", job.Name)
	if condition.Reason != "" {
		message += ": " + condition.Reason
	}
	if condition.Message != "" {
		message += ": " + condition.Message
	}
	if detail := c.podFailure(ctx, job); detail != "" {
		message += "; " + detail
	}
	return message
}

func (c *Controller) podFailure(ctx context.Context, job *batchv1.Job) string {
	ctx, cancel := context.WithTimeout(ctx, cronradar.RequestTimeout)
	defer cancel()
	var pods []corev1.Pod
	for _, label := range []string{"batch.kubernetes.io/controller-uid", "controller-uid"} {
		list, err := c.kube.CoreV1().Pods(job.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: label + "=" + string(job.UID),
		})
		if err != nil {
			c.log.Warn("listing the failed Job's pods failed; reporting the failure without exit codes", "job", job.Namespace+"/"+job.Name, "error", err)
			return ""
		}
		if len(list.Items) > 0 {
			pods = list.Items
			break
		}
	}
	sort.Slice(pods, func(i, j int) bool {
		return pods[j].CreationTimestamp.Before(&pods[i].CreationTimestamp)
	})
	for _, pod := range pods {
		statuses := append(append([]corev1.ContainerStatus{}, pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...)
		for _, status := range statuses {
			if terminated := status.State.Terminated; terminated != nil && terminated.ExitCode != 0 {
				detail := fmt.Sprintf("pod %s: container %s exited with code %d", pod.Name, status.Name, terminated.ExitCode)
				if terminated.Reason != "" {
					detail += " (" + terminated.Reason + ")"
				}
				return detail
			}
		}
		if pod.Status.Phase == corev1.PodFailed && pod.Status.Reason != "" {
			return fmt.Sprintf("pod %s: %s", pod.Name, pod.Status.Reason)
		}
	}
	return ""
}

// sendNext reports one queued event, requeueing it with backoff when CronRadar or the network
// fails transiently. It returns false once the queue shuts down.
func (c *Controller) sendNext(ctx context.Context) bool {
	event, shutdown := c.events.Get()
	if shutdown {
		return false
	}
	defer c.events.Done(event)
	if err := c.limiter.Wait(ctx); err != nil {
		return true
	}
	err := c.api.Report(ctx, event)
	if err == nil {
		c.events.Forget(event)
		c.log.Info("reported", "event", event.Kind, "key", event.MonitorKey, "run", event.RunID)
		return true
	}
	if cronradar.Retryable(err) && c.events.NumRequeues(event) < maxRetries && ctx.Err() == nil {
		c.log.Warn("report failed; retrying", "event", event.Kind, "key", event.MonitorKey, "error", err)
		c.events.AddRateLimited(event)
		return true
	}
	c.events.Forget(event)
	c.log.Error("report dropped", "event", event.Kind, "key", event.MonitorKey, "run", event.RunID, "error", err)
	return true
}
