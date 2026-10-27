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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockControlClient struct {
	ateapipb.ControlClient

	mu                  sync.Mutex
	deleteCalls         []*ateapipb.DeleteActorRequest
	getCalls            []*ateapipb.GetActorRequest
	deleteErr           error
	actorRemaining      int // Number of GetActor calls before returning NotFound
	resumeErrs          []error
	resumeActor         *ateapipb.Actor
	deleteTemplateErrs  []error
	deleteTemplateCalls int
	templates           map[string]*ateapipb.ActorTemplate
	createTemplateErrs  []error
	createdTemplates    []*ateapipb.ActorTemplate
	createActorErrs     []error
	createdActors       []*ateapipb.Actor
	egressPolicies      []*ateapipb.CreateActorEgressPolicyRequest
}

func (m *mockControlClient) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.createActorErrs) > 0 {
		err := m.createActorErrs[0]
		m.createActorErrs = m.createActorErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	m.createdActors = append(m.createdActors, req.GetActor())
	return req.GetActor(), nil
}

func (m *mockControlClient) CreateActorEgressPolicy(ctx context.Context, req *ateapipb.CreateActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapipb.EgressPolicy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.egressPolicies = append(m.egressPolicies, req)
	return req.GetEgressPolicy(), nil
}

func (m *mockControlClient) GetActorTemplate(ctx context.Context, req *ateapipb.GetActorTemplateRequest, opts ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := req.GetActorTemplate().GetAtespace() + "/" + req.GetActorTemplate().GetName()
	if tmpl, ok := m.templates[key]; ok {
		return tmpl, nil
	}
	return nil, status.Error(codes.NotFound, "template not found")
}

func (m *mockControlClient) CreateActorTemplate(ctx context.Context, req *ateapipb.CreateActorTemplateRequest, opts ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.createTemplateErrs) > 0 {
		err := m.createTemplateErrs[0]
		m.createTemplateErrs = m.createTemplateErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	m.createdTemplates = append(m.createdTemplates, req.GetActorTemplate())
	return req.GetActorTemplate(), nil
}

func (m *mockControlClient) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.resumeErrs) > 0 {
		err := m.resumeErrs[0]
		m.resumeErrs = m.resumeErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return &ateapipb.ResumeActorResponse{Actor: m.resumeActor, Resumed: true}, nil
}

func (m *mockControlClient) DeleteActorTemplate(ctx context.Context, req *ateapipb.DeleteActorTemplateRequest, opts ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteTemplateCalls++
	if len(m.deleteTemplateErrs) > 0 {
		err := m.deleteTemplateErrs[0]
		m.deleteTemplateErrs = m.deleteTemplateErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return &ateapipb.ActorTemplate{}, nil
}

func (m *mockControlClient) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalls = append(m.deleteCalls, req)
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	return &ateapipb.Actor{}, nil
}

func (m *mockControlClient) GetActor(ctx context.Context, req *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getCalls = append(m.getCalls, req)
	if m.actorRemaining > 0 {
		m.actorRemaining--
		return &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: req.GetActor().GetAtespace(),
				Name:     req.GetActor().GetName(),
			},
			Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
		}, nil
	}
	return nil, status.Error(codes.NotFound, "actor not found")
}

func TestClient_DeleteActor_AnyStateTrue(t *testing.T) {
	mock := &mockControlClient{
		actorRemaining: 0,
	}
	c := &Client{control: mock}
	ctx := context.Background()

	err := c.DeleteActor(ctx, "test-ns", "test-actor")
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.deleteCalls) != 1 {
		t.Fatalf("expected 1 DeleteActor call, got %d", len(mock.deleteCalls))
	}
	req := mock.deleteCalls[0]
	if !req.GetAnyState() {
		t.Errorf("expected AnyState to be true, got %v", req.GetAnyState())
	}
	if req.GetActor().GetAtespace() != "test-ns" || req.GetActor().GetName() != "test-actor" {
		t.Errorf("unexpected actor ref in request: %v", req.GetActor())
	}
}

func TestClient_DeleteActor_PollsUntilNotFound(t *testing.T) {
	oldInterval := defaultRetryInterval
	defaultRetryInterval = 10 * time.Millisecond
	defer func() { defaultRetryInterval = oldInterval }()

	mock := &mockControlClient{
		actorRemaining: 3,
	}
	c := &Client{control: mock}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := c.DeleteActor(ctx, "test-ns", "test-actor")
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	// 1 fast-path GetActor call + 3 polling calls (last one returns NotFound)
	if len(mock.getCalls) != 4 {
		t.Errorf("expected 4 GetActor calls before NotFound, got %d", len(mock.getCalls))
	}
}

func TestClient_DeleteActor_NotFoundIsIgnored(t *testing.T) {
	mock := &mockControlClient{
		deleteErr: status.Error(codes.NotFound, "actor already gone"),
	}
	c := &Client{control: mock}
	ctx := context.Background()

	err := c.DeleteActor(ctx, "test-ns", "test-actor")
	if err != nil {
		t.Fatalf("expected nil error when actor is NotFound, got: %v", err)
	}
}

func TestClient_DeleteActor_PropagatesRPCError(t *testing.T) {
	mock := &mockControlClient{
		deleteErr: errors.New("internal atelet failure"),
	}
	c := &Client{control: mock}
	ctx := context.Background()

	err := c.DeleteActor(ctx, "test-ns", "test-actor")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestBuildActorTemplate(t *testing.T) {
	t.Setenv("AX_SNAPSHOTS_BUCKET", "gs://ate-snapshots/ax/")

	tmpl := BuildActorTemplate("", "test-tmpl", "", map[string]string{"FOO": "BAR"}, nil, "", Limits{CPU: "500m", Memory: "1Gi"})
	if tmpl.GetMetadata().GetAtespace() != "default" {
		t.Errorf("expected default atespace, got %q", tmpl.GetMetadata().GetAtespace())
	}
	if len(tmpl.GetContainers()) != 1 {
		t.Fatalf("expected 1 container, got %d", len(tmpl.GetContainers()))
	}
	container := tmpl.GetContainers()[0]
	if container.GetImage() != v1alpha1.DefaultTaskImage {
		t.Errorf("unexpected image: %q", container.GetImage())
	}
	probe := container.GetWakeupProbe().GetHttpGet()
	if probe == nil || probe.GetPath() != "/readyz" || probe.GetPort() != 80 {
		t.Errorf("unexpected wakeupProbe: %+v", container.GetWakeupProbe())
	}
	snapCfg := tmpl.GetSnapshotConfig()
	if snapCfg == nil {
		t.Fatal("expected SnapshotConfig to be set")
	}
	if snapCfg.GetStorageLocation() != "gs://ate-snapshots/ax/" {
		t.Errorf("expected StorageLocation gs://ate-snapshots/ax/, got %q", snapCfg.GetStorageLocation())
	}
	if snapCfg.GetPreferredFidelity() != ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES {
		t.Errorf("expected PreferredFidelity VOLUMES, got %v", snapCfg.GetPreferredFidelity())
	}
	got := map[string]string{}
	for _, l := range tmpl.GetResources().GetLimits() {
		got[l.GetName()] = l.GetQuantity()
	}
	if len(got) != 2 || got["cpu"] != "500m" || got["memory"] != "1Gi" {
		t.Errorf("resources.limits = %v, want cpu=500m memory=1Gi", got)
	}

	if bare := BuildActorTemplate("", "bare", "", nil, nil, "", Limits{}); bare.GetResources() != nil {
		t.Errorf("a template without limits got resources %v", bare.GetResources())
	}
}

func TestClient_ResumeActor_WorkerPodIpsAndRetry(t *testing.T) {
	oldInterval := defaultRetryInterval
	defaultRetryInterval = 5 * time.Millisecond
	defer func() { defaultRetryInterval = oldInterval }()

	mock := &mockControlClient{
		resumeErrs: []error{
			status.Error(codes.Aborted, "operation aborted"),
			nil,
		},
		resumeActor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "task-1"},
			Status: &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
				WorkerAssignment: &ateapipb.WorkerAssignment{
					// The first (primary) IP is used, even when it is IPv6.
					WorkerPodIps: []string{"fd00::99", "10.244.0.99"},
				},
			},
		},
	}
	c := &Client{control: mock}
	actor, ip, err := c.ResumeActor(context.Background(), "default", "task-1")
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	if actor == nil {
		t.Fatal("expected non-nil actor")
	}
	if ip != "fd00::99" {
		t.Errorf("expected primary worker IP fd00::99, got %q", ip)
	}
}

func TestClient_ResumeActor_ResourceExhaustedNotRetried(t *testing.T) {
	mock := &mockControlClient{
		resumeErrs: []error{
			status.Error(codes.ResourceExhausted, "no worker available"),
		},
	}
	c := &Client{control: mock}
	_, _, err := c.ResumeActor(context.Background(), "default", "task-1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if status.Code(err) != codes.ResourceExhausted {
		t.Errorf("expected ResourceExhausted error, got %v", err)
	}
}

func TestClient_DeleteActorTemplate_RetriesOnAborted(t *testing.T) {
	oldInterval := defaultRetryInterval
	defaultRetryInterval = 5 * time.Millisecond
	defer func() { defaultRetryInterval = oldInterval }()

	mock := &mockControlClient{
		deleteTemplateErrs: []error{
			status.Error(codes.Aborted, "another operation is in progress for this ActorTemplate"),
			status.Error(codes.Aborted, "another operation is in progress for this ActorTemplate"),
			nil,
		},
	}
	c := &Client{control: mock}
	if err := c.DeleteActorTemplate(context.Background(), "default", "tmpl-1"); err != nil {
		t.Fatalf("DeleteActorTemplate failed: %v", err)
	}
	if mock.deleteTemplateCalls != 3 {
		t.Errorf("expected 3 DeleteActorTemplate calls, got %d", mock.deleteTemplateCalls)
	}
}

func TestClient_EnsureActorTemplateWithImage_Success(t *testing.T) {
	mock := &mockControlClient{}
	c := &Client{control: mock}

	created, err := c.EnsureActorTemplateWithImage(
		context.Background(),
		v1alpha1.DefaultSystemNamespace,
		"default-template",
		"default",
		"task-1-tmpl-abcd",
		"custom-image:v1",
		Limits{CPU: "2", Memory: "4Gi"},
		map[string]string{"FOO": "BAR"},
	)
	if err != nil {
		t.Fatalf("EnsureActorTemplateWithImage failed: %v", err)
	}
	if created.GetMetadata().GetAtespace() != "default" || created.GetMetadata().GetName() != "task-1-tmpl-abcd" {
		t.Errorf("unexpected metadata: %+v", created.GetMetadata())
	}
	if len(created.GetContainers()) != 1 || created.GetContainers()[0].GetImage() != "custom-image:v1" {
		t.Errorf("expected container image custom-image:v1, got %+v", created.GetContainers())
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.createdTemplates) != 1 {
		t.Fatalf("expected 1 CreateActorTemplate request, got %d", len(mock.createdTemplates))
	}
	if r := mock.createdTemplates[0].GetResources().GetLimits(); len(r) != 2 || r[0].GetQuantity() != "2" || r[1].GetQuantity() != "4Gi" {
		t.Errorf("created template limits = %v, want cpu 2 and memory 4Gi", r)
	}
}

func TestClient_EnsureActorTemplateWithImage_PropagatesError(t *testing.T) {
	mock := &mockControlClient{
		createTemplateErrs: []error{
			status.Error(codes.InvalidArgument, "actor_template.snapshot_config: invalid field"),
		},
	}
	c := &Client{control: mock}

	_, err := c.EnsureActorTemplateWithImage(
		context.Background(),
		v1alpha1.DefaultSystemNamespace,
		"default-template",
		"default",
		"task-1-tmpl-abcd",
		"custom-image:v1",
		Limits{},
		nil,
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument error, got %v", err)
	}
	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.createdTemplates) != 0 {
		t.Errorf("expected no created templates on failure, got %d", len(mock.createdTemplates))
	}
}

func TestRetryOnCodes(t *testing.T) {
	oldInterval, oldMax := defaultRetryInterval, maxRetryInterval
	defaultRetryInterval, maxRetryInterval = time.Millisecond, 2*time.Millisecond
	defer func() { defaultRetryInterval, maxRetryInterval = oldInterval, oldMax }()

	t.Run("non-retryable returns immediately", func(t *testing.T) {
		calls := 0
		wantErr := status.Error(codes.Internal, "boom")
		err := retryOnCodes(context.Background(), func() error {
			calls++
			return wantErr
		}, codes.Aborted)
		if err != wantErr || calls != 1 {
			t.Errorf("got err=%v (%T) calls=%d, want original Internal error after 1 call", err, err, calls)
		}
	})

	t.Run("non-retryable on final attempt is unwrapped", func(t *testing.T) {
		calls := 0
		wantErr := status.Error(codes.Internal, "boom")
		err := retryOnCodes(context.Background(), func() error {
			calls++
			if calls == defaultRetryAttempts {
				return wantErr
			}
			return status.Error(codes.Aborted, "busy")
		}, codes.Aborted)
		if err != wantErr || calls != defaultRetryAttempts {
			t.Errorf("got err=%v (%T) calls=%d, want original Internal error after %d calls", err, err, calls, defaultRetryAttempts)
		}
	})

	t.Run("exhausts attempts", func(t *testing.T) {
		calls := 0
		err := retryOnCodes(context.Background(), func() error {
			calls++
			return status.Error(codes.Aborted, "busy")
		}, codes.Aborted)
		if status.Code(err) != codes.Aborted || calls != defaultRetryAttempts {
			t.Errorf("got err=%v calls=%d, want Aborted after %d calls", err, calls, defaultRetryAttempts)
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := retryOnCodes(ctx, func() error {
			return status.Error(codes.Aborted, "busy")
		}, codes.Aborted)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("retries Unavailable and succeeds", func(t *testing.T) {
		calls := 0
		err := retryOnCodes(context.Background(), func() error {
			calls++
			if calls < 3 {
				return status.Error(codes.Unavailable, "ateapi rolling out")
			}
			return nil
		}, codes.Unavailable)
		if err != nil || calls != 3 {
			t.Errorf("got err=%v calls=%d, want nil after 3 calls", err, calls)
		}
	})
}

func TestIsTransientError(t *testing.T) {
	if IsTransientError(nil) {
		t.Error("expected nil error to be non-transient")
	}
	if !IsTransientError(status.Error(codes.Unavailable, "connection refused")) {
		t.Error("expected codes.Unavailable to be transient")
	}
	if !IsTransientError(status.Error(codes.DeadlineExceeded, "rpc deadline exceeded")) {
		t.Error("expected codes.DeadlineExceeded to be transient")
	}
	if !IsTransientError(context.DeadlineExceeded) {
		t.Error("expected context.DeadlineExceeded to be transient")
	}
	if IsTransientError(status.Error(codes.InvalidArgument, "bad spec")) {
		t.Error("expected codes.InvalidArgument to be non-transient")
	}
}

func TestClient_EnsureActorTemplateWithImage_InheritsBaseAndWaitsGolden(t *testing.T) {
	oldPoll := goldenSnapshotPollInterval
	goldenSnapshotPollInterval = 5 * time.Millisecond
	defer func() { goldenSnapshotPollInterval = oldPoll }()

	baseTmpl := &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "otel-bench", Name: "finops-e2e-agent"},
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"ate.dev/pool": "finops-e2e"},
		},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation:   "gs://snapshot-substrate-test-jeffluoo-gke-dev/otel-bench/finops-e2e-agent/",
			PreferredFidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
		},
		Status: &ateapipb.ActorTemplateStatus{
			GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
				GoldenTag: &ateapipb.ObjectRef{Name: "golden-1"},
			},
		},
	}
	mock := &mockControlClient{
		templates: map[string]*ateapipb.ActorTemplate{
			"otel-bench/finops-e2e-agent": baseTmpl,
		},
	}
	c := &Client{control: mock}

	// Simulate golden snapshot completing shortly after CreateActorTemplate.
	go func() {
		time.Sleep(15 * time.Millisecond)
		mock.mu.Lock()
		mock.templates["otel-bench/task-1-tmpl-abcd"] = &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "otel-bench", Name: "task-1-tmpl-abcd"},
			Status: &ateapipb.ActorTemplateStatus{
				GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
					GoldenTag: &ateapipb.ObjectRef{Name: "golden-2"},
				},
			},
		}
		mock.mu.Unlock()
	}()

	got, err := c.EnsureActorTemplateWithImage(
		context.Background(),
		"otel-bench",
		"finops-e2e-agent",
		"otel-bench",
		"task-1-tmpl-abcd",
		"custom-image:v1",
		Limits{CPU: "250m", Memory: "512Mi"},
		nil,
	)
	if err != nil {
		t.Fatalf("EnsureActorTemplateWithImage failed: %v", err)
	}
	if got.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag().GetName() != "golden-2" {
		t.Errorf("expected golden-2 tag, got %+v", got.GetStatus())
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.createdTemplates) != 1 {
		t.Fatalf("expected 1 created template, got %d", len(mock.createdTemplates))
	}
	created := mock.createdTemplates[0]
	if created.GetWorkerSelector().GetMatchLabels()["ate.dev/pool"] != "finops-e2e" {
		t.Errorf("worker selector not inherited: %+v", created.GetWorkerSelector())
	}
	wantLoc := "gs://snapshot-substrate-test-jeffluoo-gke-dev/otel-bench/task-1-tmpl-abcd/"
	if created.GetSnapshotConfig().GetStorageLocation() != wantLoc {
		t.Errorf("storage location = %q, want %q", created.GetSnapshotConfig().GetStorageLocation(), wantLoc)
	}
	if created.GetSnapshotConfig().GetPreferredFidelity() != ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS {
		t.Errorf("preferred fidelity = %v, want SYSTREE", created.GetSnapshotConfig().GetPreferredFidelity())
	}
}

func TestClient_EnsureActor_DefaultEgressPolicy(t *testing.T) {
	t.Setenv("AX_DEFAULT_EGRESS_HOSTS", "mock-e2e.ate-cost.svc.cluster.local, *.ate-cost.svc.cluster.local")
	mock := &mockControlClient{}
	c := &Client{control: mock}

	if _, err := c.EnsureActor(context.Background(), "otel-bench", "ax-parent-live", "otel-bench", "tmpl-1"); err != nil {
		t.Fatalf("EnsureActor failed: %v", err)
	}
	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.egressPolicies) != 1 {
		t.Fatalf("expected 1 egress policy, got %d", len(mock.egressPolicies))
	}
	req := mock.egressPolicies[0]
	if req.GetActor().GetAtespace() != "otel-bench" || req.GetActor().GetName() != "ax-parent-live" {
		t.Errorf("unexpected actor ref: %+v", req.GetActor())
	}
	rules := req.GetEgressPolicy().GetRules()
	if len(rules) != 1 || len(rules[0].GetHttp().GetHostnames()) != 2 {
		t.Errorf("unexpected policy rules: %+v", rules)
	}
}
