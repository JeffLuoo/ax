// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/ax/internal/auth"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/events"
	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/redis"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	goredis "github.com/redis/go-redis/v9"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var (
		listenAddr              string
		redisAddr               string
		redisPassword           string
		substrateEndpoint       string
		substrateAuthority      string
		substrateTokenFile      string
		substrateCAFile         string
		substrateInsecureTLS    bool
		substratePlaintext      bool
		defaultTemplate         string
		defaultTemplateAtespace string
		defaultTaskCPU          string
		defaultTaskMemory       string
		routerAddr              string
		taskResyncInterval      time.Duration
		outcomePollInterval     time.Duration
		outcomePollConcurrency  int
		authEnabled             bool
		authAudiences           string
		authAllowAPIServer      bool
		authCacheTTL            time.Duration
	)

	flag.StringVar(&listenAddr, "addr", ":8080", "HTTP listen address")
	flag.StringVar(&redisAddr, "redis-addr", "localhost:6379", "Redis server address")
	flag.StringVar(&redisPassword, "redis-password", "", "Redis password")
	flag.StringVar(&substrateEndpoint, "substrate-endpoint", "api.ate-system.svc.cluster.local:443", "Agent Substrate Control API endpoint")
	flag.StringVar(&substrateAuthority, "substrate-authority", "api.ate-system.svc", "Authority / TLS ServerName for Substrate endpoint")
	flag.StringVar(&substrateTokenFile, "substrate-token-file", "", "Path to bearer token file for Substrate auth")
	flag.StringVar(&substrateCAFile, "substrate-ca-file", "", "Path to CA PEM file for Substrate TLS")
	flag.BoolVar(&substrateInsecureTLS, "substrate-insecure-tls", false, "Skip Substrate TLS verification")
	flag.BoolVar(&substratePlaintext, "substrate-plaintext", false, "Use insecure plaintext gRPC connection to Substrate")
	flag.StringVar(&defaultTemplate, "template", "default-template", "Default Substrate ActorTemplate name")
	flag.StringVar(&defaultTemplateAtespace, "template-atespace", v1alpha1.DefaultSystemNamespace, "Default Substrate ActorTemplate atespace")
	flag.StringVar(&defaultTaskCPU, "default-task-cpu", controller.DefaultTaskCPU, "Sandbox CPU limit for a task that sets no spec.resources")
	flag.StringVar(&defaultTaskMemory, "default-task-memory", controller.DefaultTaskMemory, "Sandbox memory limit for a task that sets no spec.resources")
	flag.StringVar(&routerAddr, "router-addr", os.Getenv("ATENET_ROUTER_ADDR"), "atenet router host:port, for reading each running task's command state (default $ATENET_ROUTER_ADDR)")
	flag.DurationVar(&outcomePollInterval, "outcome-poll-interval", server.DefaultOutcomePollInterval, "Time between reads of the command state of running tasks; 0 turns task outcomes off")
	flag.IntVar(&outcomePollConcurrency, "outcome-poll-concurrency", server.DefaultOutcomePollConcurrency, "Most command state requests in flight at a time")
	flag.DurationVar(&taskResyncInterval, "task-resync-interval", server.DefaultTaskResyncInterval, "Time between two sends of the current state of every task over OTLP, which repair dropped task events; 0 turns the resync off")
	flag.BoolVar(&authEnabled, "auth", true, "Verify each caller's bearer token with a Kubernetes TokenReview and check each RPC with a SubjectAccessReview; false lets every caller do everything")
	flag.StringVar(&authAudiences, "auth-audiences", auth.DefaultAudience, "Comma-separated audiences that a caller's token must be for")
	flag.BoolVar(&authAllowAPIServer, "auth-allow-apiserver-tokens", false, "Also accept a token for the Kubernetes API server's own audiences, such as a kubeconfig credential")
	flag.DurationVar(&authCacheTTL, "auth-cache-ttl", auth.DefaultCacheTTL, "How long to keep a token review result")
	flag.Parse()

	for name, q := range map[string]string{"--default-task-cpu": defaultTaskCPU, "--default-task-memory": defaultTaskMemory} {
		if err := v1alpha1.ValidateQuantity(q); err != nil {
			slog.Error("invalid flag value", "flag", name, "value", q, "error", err)
			os.Exit(2)
		}
	}

	if envAddr := os.Getenv("ADDR"); envAddr != "" {
		listenAddr = envAddr
	}
	if envRedis := os.Getenv("REDIS_ADDR"); envRedis != "" {
		redisAddr = envRedis
	}
	if envPass := os.Getenv("REDIS_PASSWORD"); envPass != "" {
		redisPassword = envPass
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	slog.Info("starting ax-server",
		"listenAddr", listenAddr,
		"redisAddr", redisAddr,
		"substrateEndpoint", substrateEndpoint,
		"template", defaultTemplate,
	)

	rClient := goredis.NewClient(&goredis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
	})
	defer rClient.Close()

	rStore := redis.NewStore(rClient, redis.Options{})
	rLocker := lock.NewRedisLocker(rClient, lock.RedisLockerOptions{})

	var reconciler server.Reconciler
	subClient, err := substrate.NewClientWithOptions(substrate.ClientOptions{
		Target:      substrateEndpoint,
		Authority:   substrateAuthority,
		TokenFile:   substrateTokenFile,
		CAFile:      substrateCAFile,
		InsecureTLS: substrateInsecureTLS,
		Plaintext:   substratePlaintext,
	})
	if err != nil {
		slog.Warn("could not initialize substrate client; running without substrate reconciliation", "error", err)
	} else {
		defer subClient.Close()
		tr := controller.NewTaskReconciler(subClient, defaultTemplate, defaultTemplateAtespace)
		tr.DefaultLimits = substrate.Limits{CPU: defaultTaskCPU, Memory: defaultTaskMemory}
		reconciler = tr
	}

	// Task events go out only when an OTLP endpoint is set
	// (OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_EXPORTER_OTLP_LOGS_ENDPOINT).
	emitter, err := events.NewFromEnv(context.Background())
	if err != nil {
		slog.Warn("could not initialize the task event exporter; sending no task events", "error", err)
		emitter = events.Nop{}
	} else if events.Enabled() {
		slog.Info("sending task events over OTLP", "event", events.EventName)
	}

	var guard *auth.Guard
	if authEnabled {
		// In the cluster this is the pod's service account; outside it,
		// $KUBECONFIG.
		cfg, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
		if err != nil {
			slog.Error("could not load the Kubernetes client config for --auth", "error", err)
			os.Exit(1)
		}
		kube, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			slog.Error("could not create the Kubernetes client for --auth", "error", err)
			os.Exit(1)
		}
		var audiences []string
		for _, a := range strings.Split(authAudiences, ",") {
			if a = strings.TrimSpace(a); a != "" {
				audiences = append(audiences, a)
			}
		}
		if len(audiences) == 0 {
			slog.Error("--auth-audiences names no audience")
			os.Exit(2)
		}
		guard = &auth.Guard{
			Authn: auth.NewTokenReviewAuthenticator(kube, auth.TokenReviewOptions{
				Audiences:            audiences,
				AllowAPIServerTokens: authAllowAPIServer,
				CacheTTL:             authCacheTTL,
			}),
			Authz: auth.NewSubjectAccessReviewAuthorizer(kube),
		}
		slog.Info("authenticating callers with TokenReview", "audiences", audiences, "allowAPIServerTokens", authAllowAPIServer)
	} else {
		slog.Warn("--auth=false: every caller can act on every atespace with ax-server's Substrate credentials")
	}

	// A suspend or a delete reads the outcome of a running task first, but
	// only when task outcomes are on.
	outcomeRouterAddr := routerAddr
	if outcomePollInterval <= 0 {
		outcomeRouterAddr = ""
	}
	srv := server.NewServer(rStore, server.Options{
		Locker:     rLocker,
		Reconciler: reconciler,
		Events:     emitter,
		Guard:      guard,
		RouterAddr: outcomeRouterAddr,
	})

	httpServer := &http.Server{
		Addr:    listenAddr,
		Handler: srv.Handler(),
	}
	httpServer.Protocols = new(http.Protocols)
	httpServer.Protocols.SetHTTP1(true)
	httpServer.Protocols.SetUnencryptedHTTP2(true)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if outcomePollInterval > 0 {
		go srv.RunOutcomePoller(ctx, server.OutcomePollerOptions{
			RouterAddr:  routerAddr,
			Interval:    outcomePollInterval,
			Concurrency: outcomePollConcurrency,
		})
	}

	if events.Enabled() && taskResyncInterval > 0 {
		go srv.RunTaskResync(ctx, server.TaskResyncOptions{Interval: taskResyncInterval})
	}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down ax-server")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	if err := emitter.Shutdown(shutdownCtx); err != nil {
		slog.Warn("flushing task events", "error", err)
	}
}
