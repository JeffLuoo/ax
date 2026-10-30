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
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/ax/internal/auth"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/events"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/postgres"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/jackc/pgx/v5/pgconn"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var (
		listenAddr              string
		storeURL                string
		substrateEndpoint       string
		substrateAuthority      string
		substrateTokenFile      string
		substrateCAFile         string
		substrateInsecureTLS    bool
		substratePlaintext      bool
		defaultTemplate         string
		defaultTemplateAtespace string
		defaultTaskImage        string
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
	flag.StringVar(&storeURL, "store", "postgres://ax@localhost:5432/ax?sslmode=disable", "PostgreSQL connection URL (env: AX_STORE). libpq variables such as PGPASSWORD fill in what it leaves out")
	flag.StringVar(&substrateEndpoint, "substrate-endpoint", "api.ate-system.svc.cluster.local:443", "Agent Substrate Control API endpoint")
	flag.StringVar(&substrateAuthority, "substrate-authority", "api.ate-system.svc", "Authority / TLS ServerName for Substrate endpoint")
	flag.StringVar(&substrateTokenFile, "substrate-token-file", "", "Path to bearer token file for Substrate auth")
	flag.StringVar(&substrateCAFile, "substrate-ca-file", "", "Path to CA PEM file for Substrate TLS")
	flag.BoolVar(&substrateInsecureTLS, "substrate-insecure-tls", false, "Skip Substrate TLS verification")
	flag.BoolVar(&substratePlaintext, "substrate-plaintext", false, "Use insecure plaintext gRPC connection to Substrate")
	flag.StringVar(&defaultTemplate, "template", "default-template", "Default Substrate ActorTemplate name")
	flag.StringVar(&defaultTemplateAtespace, "template-atespace", v1alpha1.DefaultSystemNamespace, "Default Substrate ActorTemplate atespace")
	flag.StringVar(&defaultTaskImage, "default-task-image", v1alpha1.DefaultTaskImage, "Container image for tasks that do not set spec.image (env: AX_DEFAULT_TASK_IMAGE)")
	flag.StringVar(&routerAddr, "router-addr", os.Getenv("ATENET_ROUTER_ADDR"), "atenet router address (URL, or host:port where port 443 means https), for reading each running task's command state (default $ATENET_ROUTER_ADDR)")
	flag.DurationVar(&outcomePollInterval, "outcome-poll-interval", server.DefaultOutcomePollInterval, "Time between reads of the command state of running tasks; 0 turns task outcomes off")
	flag.IntVar(&outcomePollConcurrency, "outcome-poll-concurrency", server.DefaultOutcomePollConcurrency, "Most command state requests in flight at a time")
	flag.DurationVar(&taskResyncInterval, "task-resync-interval", server.DefaultTaskResyncInterval, "Time between two sends of the current state of every task over OTLP, which repair dropped task events; 0 turns the resync off")
	flag.BoolVar(&authEnabled, "auth", true, "Verify each caller's bearer token with a Kubernetes TokenReview and check each RPC with a SubjectAccessReview; false lets every caller do everything")
	flag.StringVar(&authAudiences, "auth-audiences", auth.DefaultAudience, "Comma-separated audiences that a caller's token must be for")
	flag.BoolVar(&authAllowAPIServer, "auth-allow-apiserver-tokens", false, "Also accept a token for the Kubernetes API server's own audiences, such as a kubeconfig credential")
	flag.DurationVar(&authCacheTTL, "auth-cache-ttl", auth.DefaultCacheTTL, "How long to keep a token review result")
	flag.Parse()

	if envAddr := os.Getenv("ADDR"); envAddr != "" {
		listenAddr = envAddr
	}
	// TODO: Reject or warn on passwords embedded in the store URL. They end up
	// in process args and pod specs; credentials should come from PGPASSWORD,
	// a password file, or a Secret instead.
	if envStore := os.Getenv("AX_STORE"); envStore != "" {
		storeURL = envStore
	}
	if envImage := os.Getenv("AX_DEFAULT_TASK_IMAGE"); envImage != "" {
		defaultTaskImage = envImage
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	slog.Info("starting ax-server",
		"listenAddr", listenAddr,
		"store", describeStore(storeURL),
		"substrateEndpoint", substrateEndpoint,
		"template", defaultTemplate,
		"defaultTaskImage", defaultTaskImage,
	)

	openCtx, openCancel := context.WithTimeout(context.Background(), time.Minute)
	backend, err := postgres.Open(openCtx, storeURL, postgres.Options{})
	openCancel()
	if err != nil {
		slog.Error("opening store", "store", describeStore(storeURL), "error", err)
		os.Exit(1)
	}
	defer backend.Close()

	subClient, err := substrate.NewClientWithOptions(substrate.ClientOptions{
		Target:      substrateEndpoint,
		Authority:   substrateAuthority,
		TokenFile:   substrateTokenFile,
		CAFile:      substrateCAFile,
		InsecureTLS: substrateInsecureTLS,
		Plaintext:   substratePlaintext,
	})
	if err != nil {
		slog.Error("initializing substrate client", "error", err)
		os.Exit(1)
	}
	defer subClient.Close()

	reconciler := controller.NewTaskReconciler(subClient, defaultTemplate, defaultTemplateAtespace, defaultTaskImage)
	// Budget labels follow spec.parent up to the task that owns the tree's
	// budget.
	reconciler.Tasks = backend.GetTask

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
	srv := server.NewServer(backend, reconciler, server.Options{
		Locker:     backend,
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

// describeStore summarizes a store connection string for logs. It parses the
// string the way the store does, so URLs, query parameters and keyword/value
// DSNs are all handled, and reports only the user, host, port and database.
// Passwords and other connection parameters are never included.
func describeStore(connString string) string {
	cfg, err := pgconn.ParseConfig(connString)
	if err != nil {
		return "<unparseable>"
	}
	return fmt.Sprintf("%s@%s/%s", cfg.User, net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port))), cfg.Database)
}
