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

package server_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/auth"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	authnv1 "k8s.io/api/authentication/v1"
	authzv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// rbac is a fake Kubernetes API for the guard: tokens name users, and grants
// list "user verb resource namespace" rules; namespace "" is a cluster-wide
// grant.
type rbac struct {
	mu      sync.Mutex
	tokens  map[string]string
	grants  map[string]bool
	sarFail bool
}

func (r *rbac) client() *fake.Clientset {
	c := fake.NewClientset()
	c.PrependReactor("create", "tokenreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		tr := a.(k8stesting.CreateAction).GetObject().(*authnv1.TokenReview).DeepCopy()
		if user, ok := r.tokens[tr.Spec.Token]; ok {
			tr.Status = authnv1.TokenReviewStatus{Authenticated: true, Audiences: tr.Spec.Audiences, User: authnv1.UserInfo{Username: user}}
		}
		return true, tr, nil
	})
	c.PrependReactor("create", "subjectaccessreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.sarFail {
			return true, nil, errors.New("the API server is down")
		}
		sar := a.(k8stesting.CreateAction).GetObject().(*authzv1.SubjectAccessReview).DeepCopy()
		ra := sar.Spec.ResourceAttributes
		if ra.Group == auth.Group {
			sar.Status.Allowed = r.grants[sar.Spec.User+" "+ra.Verb+" "+ra.Resource+" "+ra.Namespace] ||
				r.grants[sar.Spec.User+" "+ra.Verb+" "+ra.Resource+" "]
		}
		return true, sar, nil
	})
	return c
}

func TestGuard_GRPC(t *testing.T) {
	fakeAPI := &rbac{
		tokens: map[string]string{"alice-token": "alice", "bob-token": "bob", "admin-token": "admin"},
		grants: map[string]bool{
			"alice create tasks team-a": true,
			"alice get tasks team-a":    true,
			"alice list tasks team-a":   true,
			"bob get tasks team-a":      true,
			"admin list tasks ":         true,
		},
	}
	kube := fakeAPI.client()
	ms := memory.NewStore()
	srv := server.NewServer(ms, newTestReconciler(substratetest.NewServer(t)), server.Options{Guard: &auth.Guard{
		Authn: auth.NewTokenReviewAuthenticator(kube, auth.TokenReviewOptions{}),
		Authz: auth.NewSubjectAccessReviewAuthorizer(kube),
	}})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: srv.Handler()}
	httpServer.Protocols = new(http.Protocols)
	httpServer.Protocols.SetHTTP1(true)
	httpServer.Protocols.SetUnencryptedHTTP2(true)
	go func() { _ = httpServer.Serve(ln) }()
	defer httpServer.Close()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := v1alpha1.NewAXClient(conn)
	base, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	as := func(token string) context.Context {
		return metadata.AppendToOutgoingContext(base, "authorization", "Bearer "+token)
	}
	newTask := func(atespace, name string) *v1alpha1.CreateTaskRequest {
		return &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: name, Atespace: atespace}, Spec: &v1alpha1.TaskSpec{}}}
	}

	// /healthz needs no token.
	resp, err := http.Get("http://" + ln.Addr().String() + "/healthz")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz = %v, %v", resp, err)
	}
	resp.Body.Close()

	if _, err := client.CreateTask(base, newTask("team-a", "t1")); status.Code(err) != codes.Unauthenticated {
		t.Errorf("no token: got %v, want Unauthenticated", err)
	}
	if _, err := client.CreateTask(as("forged"), newTask("team-a", "t1")); status.Code(err) != codes.Unauthenticated {
		t.Errorf("unknown token: got %v, want Unauthenticated", err)
	}
	if _, err := client.CreateTask(as("bob-token"), newTask("team-a", "t1")); status.Code(err) != codes.PermissionDenied {
		t.Errorf("bob creates in team-a: got %v, want PermissionDenied", err)
	}
	if _, err := client.CreateTask(as("alice-token"), newTask("team-b", "t1")); status.Code(err) != codes.PermissionDenied {
		t.Errorf("alice creates in team-b: got %v, want PermissionDenied", err)
	}
	created, err := client.CreateTask(as("alice-token"), newTask("team-a", "t1"))
	if err != nil {
		t.Fatalf("alice creates in team-a: %v", err)
	}
	if created.GetStatus().GetCreatedBy() != "alice" {
		t.Errorf("created_by = %q, want alice", created.GetStatus().GetCreatedBy())
	}
	if _, err := client.GetTask(as("bob-token"), &v1alpha1.GetTaskRequest{Atespace: "team-a", Name: "t1"}); err != nil {
		t.Errorf("bob gets in team-a: %v", err)
	}
	if _, err := client.DeleteTask(as("bob-token"), &v1alpha1.DeleteTaskRequest{Atespace: "team-a", Name: "t1"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("bob deletes in team-a: got %v, want PermissionDenied", err)
	}

	// A list with no atespace covers all atespaces and needs a cluster-wide grant.
	if _, err := client.ListTasks(as("alice-token"), &v1alpha1.ListTasksRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("alice lists all atespaces: got %v, want PermissionDenied", err)
	}
	if _, err := client.ListTasks(as("alice-token"), &v1alpha1.ListTasksRequest{Atespace: "team-a"}); err != nil {
		t.Errorf("alice lists team-a: %v", err)
	}
	if got, err := client.ListTasks(as("admin-token"), &v1alpha1.ListTasksRequest{}); err != nil || len(got.GetTasks()) != 1 {
		t.Errorf("admin lists all atespaces = %v, %v", got, err)
	}

	// An authorization API error fails closed.
	fakeAPI.mu.Lock()
	fakeAPI.sarFail = true
	fakeAPI.mu.Unlock()
	if _, err := client.GetTask(as("alice-token"), &v1alpha1.GetTaskRequest{Atespace: "team-a", Name: "t1"}); status.Code(err) != codes.Unavailable {
		t.Errorf("SubjectAccessReview error: got %v, want Unavailable", err)
	}
}

// TestGuard_RulesCoverEveryRPC checks that each AX RPC has an RBAC rule, so a
// new RPC is not denied by mistake, or added without a decision.
func TestGuard_RulesCoverEveryRPC(t *testing.T) {
	for _, m := range v1alpha1.AX_ServiceDesc.Methods {
		if _, ok := auth.Rules["/"+v1alpha1.AX_ServiceDesc.ServiceName+"/"+m.MethodName]; !ok {
			t.Errorf("no auth rule for %s", m.MethodName)
		}
	}
	for _, s := range v1alpha1.AX_ServiceDesc.Streams {
		if _, ok := auth.Rules["/"+v1alpha1.AX_ServiceDesc.ServiceName+"/"+s.StreamName]; !ok {
			t.Errorf("no auth rule for %s", s.StreamName)
		}
	}
}
