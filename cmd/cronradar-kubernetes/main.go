// Command cronradar-kubernetes watches Kubernetes CronJobs and reports them to CronRadar.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/notinferred/cronradar-kubernetes/internal/controller"
	"github.com/notinferred/cronradar-kubernetes/internal/cronradar"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "0.1.0"

func main() {
	os.Exit(run())
}

func run() int {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	syncInterval, err := envDuration("CRONRADAR_SYNC_INTERVAL", 5*time.Minute)
	if err != nil {
		log.Error(err.Error())
		return 2
	}
	namespaces := flag.String("namespaces", os.Getenv("CRONRADAR_NAMESPACES"), "comma-separated namespaces to watch; empty watches all (env CRONRADAR_NAMESPACES)")
	source := flag.String("source", envOr("CRONRADAR_SOURCE", "kubernetes"), "sync source; give each cluster sharing one CronRadar application its own (env CRONRADAR_SOURCE)")
	timeZone := flag.String("time-zone", envOr("CRONRADAR_TIME_ZONE", "UTC"), "the CronRadar application's time zone, to warn about CronJobs scheduled in another (env CRONRADAR_TIME_ZONE)")
	flag.DurationVar(&syncInterval, "sync-interval", syncInterval, "full registry sync interval, at least 1m (env CRONRADAR_SYNC_INTERVAL)")
	healthAddress := flag.String("health-address", envOr("CRONRADAR_HEALTH_ADDRESS", ":8080"), "address for /healthz and /readyz; empty disables (env CRONRADAR_HEALTH_ADDRESS)")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return 0
	}
	if syncInterval < time.Minute {
		log.Error("sync interval must be at least 1m; CronRadar accepts 10 syncs a minute per key", "interval", syncInterval)
		return 2
	}
	apiKey := strings.TrimSpace(os.Getenv("CRONRADAR_API_KEY"))
	if apiKey == "" {
		log.Error("CRONRADAR_API_KEY is not set; create the Secret described in the README")
		return 2
	}

	restConfig, err := kubeConfig()
	if err != nil {
		log.Error("no Kubernetes configuration", "error", err)
		return 1
	}
	kube, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.Error("Kubernetes client failed", "error", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	watcher := controller.New(controller.Config{
		Namespaces:          splitList(*namespaces),
		Source:              strings.TrimSpace(*source),
		Version:             version,
		ApplicationTimeZone: strings.TrimSpace(*timeZone),
		SyncInterval:        syncInterval,
		SyncDebounce:        10 * time.Second,
	}, kube, cronradar.NewClient(os.Getenv("CRONRADAR_BASE_URL"), apiKey, version), log)

	if *healthAddress != "" {
		go serveHealth(ctx, *healthAddress, watcher.Ready, log)
	}
	log.Info("cronradar-kubernetes starting", "version", version)
	if err := watcher.Run(ctx); err != nil {
		log.Error("stopped", "error", err)
		return 1
	}
	return 0
}

// kubeConfig uses the pod's service account, or a kubeconfig when run outside a cluster.
func kubeConfig() (*rest.Config, error) {
	if config, err := rest.InClusterConfig(); err == nil {
		return config, nil
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
}

func serveHealth(ctx context.Context, address string, ready func() bool, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("health endpoint stopped", "error", err)
	}
}

func splitList(value string) []string {
	var items []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not a duration such as 5m: %w", name, value, err)
	}
	return duration, nil
}
