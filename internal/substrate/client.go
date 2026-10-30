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

package substrate

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ClientOptions configures connection to the Substrate Control API server.
type ClientOptions struct {
	Target      string
	Authority   string
	TokenFile   string
	CAFile      string
	InsecureTLS bool
	Plaintext   bool
}

type tokenAuth struct {
	tokenFile string
}

func (t tokenAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	if t.tokenFile == "" {
		return nil, nil
	}
	b, err := os.ReadFile(t.tokenFile)
	if err != nil {
		return nil, fmt.Errorf("reading token file %q: %w", t.tokenFile, err)
	}
	return map[string]string{
		"authorization": "Bearer " + strings.TrimSpace(string(b)),
	}, nil
}

func (t tokenAuth) RequireTransportSecurity() bool {
	return false
}

// Client wraps the Substrate Control API client.
type Client struct {
	conn    *grpc.ClientConn
	control ateapipb.ControlClient
}

// NewClientWithOptions connects to Substrate with full TLS and auth configuration.
func NewClientWithOptions(opts ClientOptions, extraDialOpts ...grpc.DialOption) (*Client, error) {
	if opts.Target == "" {
		opts.Target = "api.ate-system.svc.cluster.local:443"
	}

	// Auto-detect token file if not specified
	if opts.TokenFile == "" {
		if envToken := os.Getenv("SUBSTRATE_TOKEN_FILE"); envToken != "" {
			opts.TokenFile = envToken
		} else if _, err := os.Stat("/var/run/secrets/ateapi/token"); err == nil {
			opts.TokenFile = "/var/run/secrets/ateapi/token"
		}
	}

	// Auto-detect CA file if not specified
	if opts.CAFile == "" {
		if envCA := os.Getenv("SUBSTRATE_CA_FILE"); envCA != "" {
			opts.CAFile = envCA
		} else if _, err := os.Stat("/run/servicedns-ca/trust-bundle.pem"); err == nil {
			opts.CAFile = "/run/servicedns-ca/trust-bundle.pem"
		} else if _, err := os.Stat("/run/servicedns-ca/ca.pem"); err == nil {
			opts.CAFile = "/run/servicedns-ca/ca.pem"
		}
	}

	dialOpts := append([]grpc.DialOption{}, extraDialOpts...)

	if opts.Plaintext {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		tlsConfig := &tls.Config{}
		if opts.InsecureTLS {
			tlsConfig.InsecureSkipVerify = true
		}
		if opts.Authority != "" {
			tlsConfig.ServerName = opts.Authority
		} else if strings.Contains(opts.Target, "api.ate-system.svc") {
			tlsConfig.ServerName = "api.ate-system.svc"
		}

		if opts.CAFile != "" {
			caCert, err := os.ReadFile(opts.CAFile)
			if err != nil {
				return nil, fmt.Errorf("reading CA file %q: %w", opts.CAFile, err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to parse CA certificate from %q", opts.CAFile)
			}
			tlsConfig.RootCAs = pool
		}

		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	authority := opts.Authority
	if authority == "" && strings.Contains(opts.Target, "api.ate-system.svc") {
		authority = "api.ate-system.svc"
	}
	if authority != "" {
		dialOpts = append(dialOpts, grpc.WithAuthority(authority))
	}

	if opts.TokenFile != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(tokenAuth{tokenFile: opts.TokenFile}))
	}

	conn, err := grpc.NewClient(opts.Target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Substrate control service at %s: %w", opts.Target, err)
	}
	return &Client{
		conn:    conn,
		control: ateapipb.NewControlClient(conn),
	}, nil
}

// NewClient establishes a connection to the Substrate Control API server.
func NewClient(target string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) > 0 {
		if target == "" {
			target = "api.ate-system.svc.cluster.local:443"
		}
		conn, err := grpc.NewClient(target, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to Substrate control service at %s: %w", target, err)
		}
		return &Client{
			conn:    conn,
			control: ateapipb.NewControlClient(conn),
		}, nil
	}

	// Auto-configure options when no explicit dial options given
	return NewClientWithOptions(ClientOptions{Target: target})
}

// Close closes the gRPC connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// EnsureAtespace creates the atespace if it does not already exist.
func (c *Client) EnsureAtespace(ctx context.Context, atespace string) error {
	req := &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{
			Metadata: &ateapipb.ResourceMetadata{
				Name: atespace,
			},
		},
	}
	err := retryOnCodes(ctx, func() error {
		_, err := c.control.CreateAtespace(ctx, req)
		return err
	}, codes.Unavailable)
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("creating atespace %q: %w", atespace, err)
	}
	return nil
}

// GetActorTemplate fetches an ActorTemplate by name.
func (c *Client) GetActorTemplate(ctx context.Context, atespace, templateName string) (*ateapipb.ActorTemplate, error) {
	req := &ateapipb.GetActorTemplateRequest{
		ActorTemplate: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     templateName,
		},
	}
	var tmpl *ateapipb.ActorTemplate
	err := retryOnCodes(ctx, func() error {
		var err error
		tmpl, err = c.control.GetActorTemplate(ctx, req)
		return err
	}, codes.Unavailable)
	return tmpl, err
}

const (
	DefaultGuestCommand    = "/usr/local/bin/ax-task-runner"
	DefaultSnapshotsBucket = "gs://dberkov-gke-dev3/ate-env/"

	// defaultRetryAttempts is the maximum number of calls retryOnCodes makes. With
	// exponential backoff from defaultRetryInterval capped at maxRetryInterval
	// (±50% jitter), the total wait across all attempts is roughly 6s-20s.
	defaultRetryAttempts = 10

	// retryBackoffFactor is the multiplier applied to the backoff after each failed attempt.
	retryBackoffFactor = 2

	// actorDeletingWaitAttempts bounds how many times EnsureActor retries creation
	// while a previous actor with the same name is still being deleted.
	actorDeletingWaitAttempts = 20
)

var (
	// defaultRetryInterval is the initial backoff for retryOnCodes and the poll
	// interval used while waiting for actor deletion.
	defaultRetryInterval = 200 * time.Millisecond

	// maxRetryInterval caps the exponential backoff in retryOnCodes.
	maxRetryInterval = 2 * time.Second

	// actorDeletingWaitInterval is the interval between creation attempts while a
	// previous actor is still deleting (actorDeletingWaitAttempts * interval ~= 10s).
	actorDeletingWaitInterval = 500 * time.Millisecond
)

// IsTransientError reports whether err represents a transient Substrate
// control-plane unavailability or timeout rather than a permanent failure.
func IsTransientError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded:
			return true
		}
	}
	return false
}

// retryOnCodes calls fn until it succeeds, returns an error whose gRPC code is not in
// retryable, defaultRetryAttempts is exhausted, or ctx is done. Between attempts it
// waits with jittered exponential backoff starting at defaultRetryInterval and
// capped at maxRetryInterval.
func retryOnCodes(ctx context.Context, fn func() error, retryable ...codes.Code) error {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = defaultRetryInterval
	b.MaxInterval = maxRetryInterval
	b.Multiplier = retryBackoffFactor

	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		err := fn()
		if err != nil && !slices.Contains(retryable, status.Code(err)) {
			return struct{}{}, backoff.Permanent(err)
		}
		return struct{}{}, err
	}, backoff.WithBackOff(b), backoff.WithMaxTries(defaultRetryAttempts))

	// backoff.Retry returns the *PermanentError wrapper as-is when the attempt limit
	// is hit on the same try, so unwrap it to always surface the original error.
	var permanent *backoff.PermanentError
	if errors.As(err, &permanent) {
		return permanent.Unwrap()
	}
	return err
}

const (
	// DefaultCPUMillicores is the default CPU limit (1 core) applied to an
	// ActorTemplate when a Task omits spec.resources.limits.cpu.
	DefaultCPUMillicores int64 = 1000
	// DefaultMemoryBytes is the default memory limit (512 MiB) applied to an
	// ActorTemplate when a Task omits spec.resources.limits.memory.
	DefaultMemoryBytes int64 = 512 << 20
)

// ResourceLimits translates a Task's resource limits into the Substrate
// ActorTemplate resources block. Substrate sizes a sandbox by limits alone
// (requests are rejected by v1alpha1.ValidateResources). It returns nil when no
// limit is set so the template uses the default sandbox limits.
func ResourceLimits(reqs *v1alpha1.ResourcesSpec) *ateapipb.ResourceRequirements {
	limits := reqs.GetLimits()
	cpuStr := limits.GetCpu()
	memStr := limits.GetMemory()
	if cpuStr == "" && memStr == "" {
		return nil
	}
	out := &ateapipb.Resources{
		CpuMillicores: DefaultCPUMillicores,
		MemoryBytes:   DefaultMemoryBytes,
	}
	if cpuStr != "" {
		if cpu, err := v1alpha1.ParseCPUMillicores(cpuStr); err == nil {
			out.CpuMillicores = cpu
		}
	}
	if memStr != "" {
		if mem, err := v1alpha1.ParseMemoryBytes(memStr); err == nil {
			out.MemoryBytes = mem
		}
	}
	return &ateapipb.ResourceRequirements{Limits: out}
}

// BuildActorTemplate constructs a Substrate ActorTemplate based on the standard ate-env specification.
// A nil resources applies DefaultCPUMillicores and DefaultMemoryBytes.
func BuildActorTemplate(atespace, name, image string, envMap map[string]string, command []string, snapshotsBucket string, resources *ateapipb.ResourceRequirements) *ateapipb.ActorTemplate {
	if atespace == "" {
		atespace = "default"
	}
	if image == "" {
		image = v1alpha1.DefaultTaskImage
	}
	if len(command) == 0 {
		command = []string{DefaultGuestCommand}
	}
	if snapshotsBucket == "" {
		if envBucket := os.Getenv("AX_SNAPSHOTS_BUCKET"); envBucket != "" {
			snapshotsBucket = envBucket
		} else {
			snapshotsBucket = DefaultSnapshotsBucket
		}
	}

	var envList []*ateapipb.EnvVar
	for k, v := range envMap {
		envList = append(envList, &ateapipb.EnvVar{
			Name:  k,
			Value: v,
		})
	}

	cpuMillicores := DefaultCPUMillicores
	memoryBytes := DefaultMemoryBytes
	if limits := resources.GetLimits(); limits != nil {
		if limits.GetCpuMillicores() > 0 {
			cpuMillicores = limits.GetCpuMillicores()
		}
		if limits.GetMemoryBytes() > 0 {
			memoryBytes = limits.GetMemoryBytes()
		}
	}

	return &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			Name:     name,
			Atespace: atespace,
		},
		Containers: []*ateapipb.Container{{
			Name:    "guest",
			Image:   image,
			Command: command,
			Env:     envList,
			WakeupProbe: &ateapipb.WakeupProbe{
				HttpGet: &ateapipb.HttpGetAction{
					Path: "/readyz",
					Port: 80,
				},
			},
			VolumeMounts: []*ateapipb.VolumeMount{{
				Name:      "workspace",
				MountPath: "/workspace",
			}},
		}},
		Volumes: []*ateapipb.Volume{{
			Name:       "workspace",
			DurableDir: &ateapipb.DurableDirVolumeSource{},
		}},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation:   snapshotsBucket,
			PreferredFidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
		},
		Resources: &ateapipb.ResourceRequirements{
			Limits: &ateapipb.Resources{
				CpuMillicores: cpuMillicores,
				MemoryBytes:   memoryBytes,
			},
		},
	}
}

// EnsureActorTemplateWithImage creates an ActorTemplate using the specified container image,
// resource limits (nil for default limits), and optional environment variables,
// unless it already exists. The name must change whenever any of them changes,
// because a template is immutable.
func (c *Client) EnsureActorTemplateWithImage(ctx context.Context, baseAtespace, baseTemplate, targetAtespace, targetTemplate, image string, resources *ateapipb.ResourceRequirements, extraEnv ...map[string]string) (*ateapipb.ActorTemplate, error) {
	var base *ateapipb.ActorTemplate
	if baseTemplate != "" {
		base, _ = c.GetActorTemplate(ctx, baseAtespace, baseTemplate)
	}
	waitGolden := base != nil && base.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag() != nil
	existing, err := c.GetActorTemplate(ctx, targetAtespace, targetTemplate)
	if err == nil && existing != nil {
		if waitGolden && existing.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag() == nil {
			return c.waitGoldenSnapshot(ctx, targetAtespace, targetTemplate)
		}
		return existing, nil
	}

	envMap := make(map[string]string)
	for _, envs := range extraEnv {
		for k, v := range envs {
			envMap[k] = v
		}
	}

	tmpl := BuildActorTemplate(targetAtespace, targetTemplate, image, envMap, nil, "", resources)
	if base != nil {
		if tmpl.WorkerSelector == nil && base.GetWorkerSelector() != nil {
			tmpl.WorkerSelector = proto.Clone(base.GetWorkerSelector()).(*ateapipb.Selector)
		}
		if base.GetSnapshotConfig() != nil {
			sc := proto.Clone(base.GetSnapshotConfig()).(*ateapipb.SnapshotConfig)
			if base.GetSnapshotConfig().GetStorageLocation() != "" {
				loc := strings.TrimSpace(os.Getenv("AX_SNAPSHOTS_BUCKET"))
				if loc == "" {
					baseLoc := strings.TrimRight(base.GetSnapshotConfig().GetStorageLocation(), "/")
					suffix := "/" + strings.Trim(baseAtespace, "/") + "/" + strings.Trim(baseTemplate, "/")
					if strings.HasSuffix(baseLoc, suffix) {
						loc = strings.TrimSuffix(baseLoc, suffix)
					} else if idx := strings.Index(baseLoc, "://"); idx != -1 {
						rest := baseLoc[idx+3:]
						if slash := strings.IndexByte(rest, '/'); slash != -1 {
							loc = baseLoc[:idx+3+slash]
						} else {
							loc = baseLoc
						}
					}
				}
				loc = strings.TrimRight(loc, "/")
				if loc != "" {
					sc.StorageLocation = fmt.Sprintf("%s/%s/%s/", loc, targetAtespace, targetTemplate)
				}
			}
			tmpl.SnapshotConfig = sc
		}
	}
	req := &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: tmpl,
	}
	var created *ateapipb.ActorTemplate
	err = retryOnCodes(ctx, func() error {
		var err error
		created, err = c.control.CreateActorTemplate(ctx, req)
		return err
	}, codes.Unavailable)
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return nil, fmt.Errorf("creating actor template %s/%s: %w", targetAtespace, targetTemplate, err)
	}
	if waitGolden {
		return c.waitGoldenSnapshot(ctx, targetAtespace, targetTemplate)
	}
	if created != nil {
		return created, nil
	}
	return c.GetActorTemplate(ctx, targetAtespace, targetTemplate)
}

var goldenSnapshotPollInterval = 1 * time.Second

func (c *Client) waitGoldenSnapshot(ctx context.Context, atespace, templateName string) (*ateapipb.ActorTemplate, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	ticker := time.NewTicker(goldenSnapshotPollInterval)
	defer ticker.Stop()
	for {
		tmpl, err := c.GetActorTemplate(waitCtx, atespace, templateName)
		if err == nil && tmpl != nil {
			gs := tmpl.GetStatus().GetGoldenSnapshotStatus()
			if gs.GetErrorMessage() != "" {
				return nil, fmt.Errorf("golden snapshot for %s/%s failed: %s", atespace, templateName, gs.GetErrorMessage())
			}
			if gs.GetGoldenTag() != nil {
				return tmpl, nil
			}
		}
		select {
		case <-waitCtx.Done():
			return tmpl, fmt.Errorf("waiting for golden snapshot on %s/%s: %w", atespace, templateName, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

// EnsureActor creates an Actor in the specified atespace deriving from an ActorTemplate.
func (c *Client) EnsureActor(ctx context.Context, atespace, actorName, templateAtespace, templateName string) (*ateapipb.Actor, error) {
	return c.EnsureActorWithLabels(ctx, atespace, actorName, templateAtespace, templateName, nil)
}

// EnsureActorWithLabels is EnsureActor with labels set on a new actor. An
// existing actor keeps its labels; use SetActorLabels to change them.
func (c *Client) EnsureActorWithLabels(ctx context.Context, atespace, actorName, templateAtespace, templateName string, labels map[string]string) (*ateapipb.Actor, error) {
	req := &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: atespace,
				Name:     actorName,
			},
			Labels: labels,
			ActorTemplate: &ateapipb.ObjectRef{
				Atespace: templateAtespace,
				Name:     templateName,
			},
		},
	}
	var actor *ateapipb.Actor
	err := retryOnCodes(ctx, func() error {
		var err error
		actor, err = c.control.CreateActor(ctx, req)
		return err
	}, codes.Unavailable)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			existing, getErr := c.GetActor(ctx, atespace, actorName)
			if getErr == nil && existing != nil {
				state := existing.GetStatus().GetState()
				if state == ateapipb.ActorState_ACTOR_STATE_CRASHED {
					// Revert to the last snapshot so the workspace survives the crash.
					var reverted *ateapipb.RevertActorResponse
					revertErr := retryOnCodes(ctx, func() error {
						var err error
						reverted, err = c.control.RevertActor(ctx, &ateapipb.RevertActorRequest{
							Actor: &ateapipb.ObjectRef{
								Atespace: atespace,
								Name:     actorName,
							},
						})
						return err
					}, codes.Unavailable)
					if revertErr == nil {
						slog.Warn("existing actor is crashed, reverted to its last snapshot", "actor", actorName)
						c.ensureDefaultEgressPolicy(ctx, atespace, actorName)
						return reverted.GetActor(), nil
					}
					slog.Warn("existing actor is crashed and could not be reverted, deleting and recreating", "actor", actorName, "error", revertErr)
					if err := c.DeleteActor(ctx, atespace, actorName); err != nil {
						return nil, err
					}
					err = retryOnCodes(ctx, func() error {
						var err error
						actor, err = c.control.CreateActor(ctx, req)
						return err
					}, codes.Unavailable)
					if err == nil {
						c.ensureDefaultEgressPolicy(ctx, atespace, actorName)
						return actor, nil
					}
					return nil, fmt.Errorf("recreating actor %s/%s after crash: %w", atespace, actorName, err)
				}
				if state == ateapipb.ActorState_ACTOR_STATE_DELETING {
					// Wait briefly for previous actor deletion to finalize before recreating
					for i := 0; i < actorDeletingWaitAttempts; i++ {
						select {
						case <-ctx.Done():
							return nil, ctx.Err()
						case <-time.After(actorDeletingWaitInterval):
						}
						actor, err = c.control.CreateActor(ctx, req)
						if err == nil {
							c.ensureDefaultEgressPolicy(ctx, atespace, actorName)
							return actor, nil
						}
						if status.Code(err) != codes.AlreadyExists {
							break
						}
					}
					return nil, fmt.Errorf("actor %s/%s is still deleting; please retry", atespace, actorName)
				}
			}
			c.ensureDefaultEgressPolicy(ctx, atespace, actorName)
			return existing, getErr
		}
		return nil, fmt.Errorf("creating actor %s/%s: %w", atespace, actorName, err)
	}
	c.ensureDefaultEgressPolicy(ctx, atespace, actorName)
	return actor, nil
}

func (c *Client) ensureDefaultEgressPolicy(ctx context.Context, atespace, actorName string) {
	raw := strings.TrimSpace(os.Getenv("AX_DEFAULT_EGRESS_HOSTS"))
	if raw == "" {
		return
	}
	var hosts []string
	for _, h := range strings.Split(raw, ",") {
		if h = strings.TrimSpace(h); h != "" {
			hosts = append(hosts, h)
		}
	}
	if len(hosts) == 0 {
		return
	}
	_, _ = c.control.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{
		Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: actorName},
		EgressPolicy: &ateapipb.EgressPolicy{
			Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: "default"},
			Rules: []*ateapipb.EgressRule{{
				Http: &ateapipb.HttpRule{Hostnames: hosts},
			}},
		},
	})
}

// ResumeActor resumes the specified actor onto a worker and returns the worker details.
func (c *Client) ResumeActor(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, string, error) {
	req := &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	}
	var resp *ateapipb.ResumeActorResponse
	err := retryOnCodes(ctx, func() error {
		var err error
		resp, err = c.control.ResumeActor(ctx, req)
		return err
	}, codes.Aborted, codes.Unavailable)
	if err != nil {
		return nil, "", fmt.Errorf("resuming actor %s/%s: %w", atespace, actorName, err)
	}
	if resp.GetActor() == nil {
		return nil, "", fmt.Errorf("nil actor returned when resuming %s/%s", atespace, actorName)
	}
	return resp.GetActor(), WorkerIP(resp.GetActor()), nil
}

// WorkerIP returns the pod IP of the worker hosting the actor, or "" when the
// actor has no worker (for example while it is suspended).
func WorkerIP(actor *ateapipb.Actor) string {
	if ips := actor.GetStatus().GetWorkerAssignment().GetWorkerPodIps(); len(ips) > 0 {
		return ips[0]
	}
	return ""
}

// SuspendActor suspends the specified actor, triggering state checkpointing.
func (c *Client) SuspendActor(ctx context.Context, atespace, actorName string) error {
	req := &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	}
	err := retryOnCodes(ctx, func() error {
		_, err := c.control.SuspendActor(ctx, req)
		return err
	}, codes.Aborted, codes.Unavailable)
	if err != nil {
		return fmt.Errorf("suspending actor %s/%s: %w", atespace, actorName, err)
	}
	return nil
}

// GetActor fetches an Actor by name.
func (c *Client) GetActor(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, error) {
	req := &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	}
	var actor *ateapipb.Actor
	err := retryOnCodes(ctx, func() error {
		var err error
		actor, err = c.control.GetActor(ctx, req)
		return err
	}, codes.Unavailable)
	return actor, err
}

// ListActors returns every Actor in the given atespace, or in all atespaces
// when atespace is empty.
func (c *Client) ListActors(ctx context.Context, atespace string) ([]*ateapipb.Actor, error) {
	var actors []*ateapipb.Actor
	req := &ateapipb.ListActorsRequest{Atespace: atespace, Options: &ateapipb.ListOptions{}}
	for {
		var resp *ateapipb.ListActorsResponse
		err := retryOnCodes(ctx, func() error {
			var err error
			resp, err = c.control.ListActors(ctx, req)
			return err
		}, codes.Unavailable)
		if err != nil {
			return nil, fmt.Errorf("listing actors in %q: %w", atespace, err)
		}
		actors = append(actors, resp.GetActors()...)
		if resp.GetNextPageToken() == "" {
			return actors, nil
		}
		req.Options.PageToken = resp.GetNextPageToken()
	}
}

// DeleteActor deletes the specified actor from Substrate and blocks until the actor
// is fully deleted.
func (c *Client) DeleteActor(ctx context.Context, atespace, actorName string) error {
	req := &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	}
	err := retryOnCodes(ctx, func() error {
		_, err := c.control.DeleteActor(ctx, req)
		return err
	}, codes.Aborted, codes.Unavailable)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("deleting actor %s/%s: %w", atespace, actorName, err)
	}

	getReq := &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	}

	// Fast path: check if the actor was deleted synchronously.
	_, err = c.control.GetActor(ctx, getReq)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("waiting for actor %s/%s deletion: %w", atespace, actorName, ctx.Err())
		}
	}

	ticker := time.NewTicker(defaultRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for actor %s/%s deletion: %w", atespace, actorName, ctx.Err())
		case <-ticker.C:
			_, err := c.control.GetActor(ctx, getReq)
			if err != nil {
				if status.Code(err) == codes.NotFound {
					return nil
				}
				if ctx.Err() != nil {
					return fmt.Errorf("waiting for actor %s/%s deletion: %w", atespace, actorName, ctx.Err())
				}
			}
		}
	}
}

// SetActorLabels merges set into the actor's labels and removes the keys in
// del. Substrate lets each caller manage only the label prefixes that it
// owns; ax-server owns agent-cost/budget-*.
func (c *Client) SetActorLabels(ctx context.Context, atespace, actorName string, set map[string]string, del []string) (*ateapipb.Actor, error) {
	req := &ateapipb.SetActorLabelsRequest{
		Actor:      &ateapipb.ObjectRef{Atespace: atespace, Name: actorName},
		Labels:     set,
		DeleteKeys: del,
	}
	var actor *ateapipb.Actor
	err := retryOnCodes(ctx, func() error {
		var err error
		actor, err = c.control.SetActorLabels(ctx, req)
		return err
	}, codes.Aborted, codes.Unavailable)
	if err != nil {
		return nil, fmt.Errorf("setting labels on actor %s/%s: %w", atespace, actorName, err)
	}
	return actor, nil
}

// Holds returns the actor's holds as Ax task status holds.
func Holds(actor *ateapipb.Actor) []*v1alpha1.ActorHold {
	var out []*v1alpha1.ActorHold
	for _, h := range actor.GetStatus().GetHolds() {
		out = append(out, &v1alpha1.ActorHold{
			Name:      h.GetName(),
			Reason:    h.GetReason(),
			CreatedBy: h.GetCreatedBy(),
			CreatedAt: h.GetCreatedAt(),
		})
	}
	return out
}

// ActorHeldReason is the ErrorInfo reason of a resume that a hold refused.
const ActorHeldReason = "ACTOR_HELD"

// HeldError describes a resume that Substrate refused because the actor has
// a hold.
type HeldError struct {
	// Hold is the name of the first hold, for example agent-cost/budget.
	Hold string
	// Reason is that hold's reason.
	Reason string
}

func (e *HeldError) Error() string {
	if e.Reason == "" {
		return "actor held by " + e.Hold
	}
	return fmt.Sprintf("actor held by %s: %s", e.Hold, e.Reason)
}

// AsHeld returns the hold that made Substrate refuse a resume: a
// FailedPrecondition error with ErrorInfo reason ACTOR_HELD. A hold is not
// transient, so callers must not retry it.
func AsHeld(err error) (*HeldError, bool) {
	if err == nil {
		return nil, false
	}
	var held *HeldError
	if errors.As(err, &held) {
		return held, true
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return nil, false
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == ActorHeldReason {
			return &HeldError{Hold: info.GetMetadata()["hold"], Reason: info.GetMetadata()["reason"]}, true
		}
	}
	return nil, false
}

// ListActorTemplates returns all ActorTemplates in the given atespace.
func (c *Client) ListActorTemplates(ctx context.Context, atespace string) ([]*ateapipb.ActorTemplate, error) {
	var resp *ateapipb.ListActorTemplatesResponse
	err := retryOnCodes(ctx, func() error {
		var err error
		resp, err = c.control.ListActorTemplates(ctx, &ateapipb.ListActorTemplatesRequest{Atespace: atespace})
		return err
	}, codes.Unavailable)
	if err != nil {
		return nil, fmt.Errorf("listing actor templates in %s: %w", atespace, err)
	}
	if resp.GetNextPageToken() != "" {
		slog.Warn("actor template listing was truncated", "atespace", atespace)
	}
	return resp.GetActorTemplates(), nil
}

// DeleteActorTemplate deletes the specified ActorTemplate. A missing template is not an error.
func (c *Client) DeleteActorTemplate(ctx context.Context, atespace, templateName string) error {
	req := &ateapipb.DeleteActorTemplateRequest{
		ActorTemplate: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     templateName,
		},
	}
	err := retryOnCodes(ctx, func() error {
		_, err := c.control.DeleteActorTemplate(ctx, req)
		return err
	}, codes.Aborted, codes.Unavailable)
	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("deleting actor template %s/%s: %w", atespace, templateName, err)
	}
	return nil
}
