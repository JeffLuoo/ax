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

package auth_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/auth"
	authnv1 "k8s.io/api/authentication/v1"
	authzv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// tokenAPI is a fake TokenReview API. tokens maps a token to its user and to
// the audiences it is for; nil audiences mean the API server's own.
type tokenAPI struct {
	mu        sync.Mutex
	tokens    map[string]tokenInfo
	fail      error
	calls     int
	audiences [][]string
}

type tokenInfo struct {
	user      string
	audiences []string
}

func (f *tokenAPI) client() *fake.Clientset {
	c := fake.NewClientset()
	c.PrependReactor("create", "tokenreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.calls++
		tr := a.(k8stesting.CreateAction).GetObject().(*authnv1.TokenReview)
		f.audiences = append(f.audiences, tr.Spec.Audiences)
		if f.fail != nil {
			return true, nil, f.fail
		}
		info, ok := f.tokens[tr.Spec.Token]
		want := tr.Spec.Audiences
		if len(want) == 0 {
			want = []string{"https://kubernetes.default.svc"}
		}
		var match []string
		for _, a := range info.audiences {
			if slices.Contains(want, a) {
				match = append(match, a)
			}
		}
		out := tr.DeepCopy()
		if ok && len(match) > 0 {
			out.Status = authnv1.TokenReviewStatus{Authenticated: true, Audiences: match,
				User: authnv1.UserInfo{Username: info.user, UID: "uid-" + info.user, Groups: []string{"system:authenticated"}}}
		}
		return true, out, nil
	})
	return c
}

func (f *tokenAPI) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

const apiServer = "https://kubernetes.default.svc"

func newAPI() *tokenAPI {
	return &tokenAPI{tokens: map[string]tokenInfo{
		"bound":      {user: "system:serviceaccount:ci:runner", audiences: []string{"ax-server"}},
		"kubeconfig": {user: "alice@example.com", audiences: []string{apiServer}},
	}}
}

func TestTokenReview_AudienceBoundToken(t *testing.T) {
	api := newAPI()
	a := auth.NewTokenReviewAuthenticator(api.client(), auth.TokenReviewOptions{})
	p, err := a.Authenticate(context.Background(), "bound")
	if err != nil || p.Username != "system:serviceaccount:ci:runner" || p.UID == "" {
		t.Fatalf("Authenticate(bound) = %+v, %v", p, err)
	}
	if got := api.audiences[0]; !slices.Equal(got, []string{auth.DefaultAudience}) {
		t.Errorf("review audiences = %v, want [%s]", got, auth.DefaultAudience)
	}
}

func TestTokenReview_APIServerTokenNeedsFlag(t *testing.T) {
	api := newAPI()
	strict := auth.NewTokenReviewAuthenticator(api.client(), auth.TokenReviewOptions{})
	if _, err := strict.Authenticate(context.Background(), "kubeconfig"); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("kubeconfig token without the flag: err = %v, want ErrUnauthenticated", err)
	}

	api = newAPI()
	lax := auth.NewTokenReviewAuthenticator(api.client(), auth.TokenReviewOptions{AllowAPIServerTokens: true})
	p, err := lax.Authenticate(context.Background(), "kubeconfig")
	if err != nil || p.Username != "alice@example.com" {
		t.Fatalf("kubeconfig token with the flag = %+v, %v", p, err)
	}
	if len(api.audiences) != 2 || api.audiences[1] != nil {
		t.Errorf("review audiences = %v, want [ax-server] then the API server's own", api.audiences)
	}
	if _, err := lax.Authenticate(context.Background(), "unknown"); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Errorf("unknown token: err = %v, want ErrUnauthenticated", err)
	}
}

// TestTokenReview_ChecksStatusAudiences checks that a review that does not
// return a requested audience fails, also when it says Authenticated.
func TestTokenReview_ChecksStatusAudiences(t *testing.T) {
	c := fake.NewClientset()
	c.PrependReactor("create", "tokenreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		out := a.(k8stesting.CreateAction).GetObject().(*authnv1.TokenReview).DeepCopy()
		out.Status = authnv1.TokenReviewStatus{Authenticated: true, Audiences: []string{"other"}, User: authnv1.UserInfo{Username: "eve"}}
		return true, out, nil
	})
	a := auth.NewTokenReviewAuthenticator(c, auth.TokenReviewOptions{})
	if _, err := a.Authenticate(context.Background(), "t"); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("err = %v, want ErrUnauthenticated", err)
	}
}

func TestTokenReview_Cache(t *testing.T) {
	api := newAPI()
	now := time.Unix(1000, 0)
	a := auth.NewTokenReviewAuthenticator(api.client(), auth.TokenReviewOptions{CacheTTL: time.Minute, Now: func() time.Time { return now }})
	ctx := context.Background()
	for range 3 {
		if _, err := a.Authenticate(ctx, "bound"); err != nil {
			t.Fatal(err)
		}
		if _, err := a.Authenticate(ctx, "bad"); !errors.Is(err, auth.ErrUnauthenticated) {
			t.Fatalf("bad token: %v", err)
		}
	}
	if n := api.callCount(); n != 2 {
		t.Errorf("reviews = %d, want 2: a success and a rejection are each cached", n)
	}
	now = now.Add(time.Minute)
	if _, err := a.Authenticate(ctx, "bound"); err != nil {
		t.Fatal(err)
	}
	if n := api.callCount(); n != 3 {
		t.Errorf("reviews = %d after the TTL, want 3", n)
	}
}

func TestTokenReview_CacheIsBounded(t *testing.T) {
	api := newAPI()
	for i := range 10 {
		api.tokens[fmt.Sprint("t", i)] = tokenInfo{user: fmt.Sprint("u", i), audiences: []string{"ax-server"}}
	}
	a := auth.NewTokenReviewAuthenticator(api.client(), auth.TokenReviewOptions{MaxEntries: 3})
	ctx := context.Background()
	for i := range 10 {
		if _, err := a.Authenticate(ctx, fmt.Sprint("t", i)); err != nil {
			t.Fatal(err)
		}
	}
	before := api.callCount()
	for i := range 10 {
		if _, err := a.Authenticate(ctx, fmt.Sprint("t", i)); err != nil {
			t.Fatal(err)
		}
	}
	// At most 3 of the 10 tokens are still cached.
	if again := api.callCount() - before; again < 7 {
		t.Errorf("reviews on the second pass = %d, want at least 7 with a cache of 3", again)
	}
}

// TestTokenReview_APIErrorFailsClosed checks that an API error is not a
// success and not a cached rejection: the next call asks again.
func TestTokenReview_APIErrorFailsClosed(t *testing.T) {
	api := newAPI()
	api.fail = errors.New("connection refused")
	a := auth.NewTokenReviewAuthenticator(api.client(), auth.TokenReviewOptions{})
	ctx := context.Background()
	if _, err := a.Authenticate(ctx, "bound"); err == nil || errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("err = %v, want an API error that is not ErrUnauthenticated", err)
	}
	api.mu.Lock()
	api.fail = nil
	api.mu.Unlock()
	if p, err := a.Authenticate(ctx, "bound"); err != nil || p.Username == "" {
		t.Fatalf("after the API recovers = %+v, %v", p, err)
	}
}

func TestTokenReview_EmptyToken(t *testing.T) {
	api := newAPI()
	a := auth.NewTokenReviewAuthenticator(api.client(), auth.TokenReviewOptions{})
	if _, err := a.Authenticate(context.Background(), ""); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("err = %v, want ErrUnauthenticated", err)
	}
	if api.callCount() != 0 {
		t.Errorf("an empty token sent %d reviews, want 0", api.callCount())
	}
}

func TestSubjectAccessReview_SendsAttributes(t *testing.T) {
	c := fake.NewClientset()
	var got authzv1.SubjectAccessReviewSpec
	c.PrependReactor("create", "subjectaccessreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		sar := a.(k8stesting.CreateAction).GetObject().(*authzv1.SubjectAccessReview).DeepCopy()
		got = sar.Spec
		sar.Status = authzv1.SubjectAccessReviewStatus{Allowed: sar.Spec.ResourceAttributes.Namespace == "team-a", Reason: "test"}
		return true, sar, nil
	})
	z := auth.NewSubjectAccessReviewAuthorizer(c)
	p := auth.Principal{Username: "alice", UID: "u1", Groups: []string{"devs"}, Extra: map[string][]string{"k": {"v"}}}
	ok, reason, err := z.Authorize(context.Background(), p, auth.Attributes{Verb: "create", Resource: "tasks", Atespace: "team-a"})
	if err != nil || !ok || reason != "test" {
		t.Fatalf("Authorize = %v, %q, %v", ok, reason, err)
	}
	ra := got.ResourceAttributes
	if got.User != "alice" || got.UID != "u1" || !slices.Equal(got.Groups, []string{"devs"}) || got.Extra["k"][0] != "v" ||
		ra.Group != auth.Group || ra.Resource != "tasks" || ra.Verb != "create" || ra.Namespace != "team-a" {
		t.Errorf("review spec = %+v, resource attributes = %+v", got, ra)
	}
	if ok, _, _ := z.Authorize(context.Background(), p, auth.Attributes{Verb: "create", Resource: "tasks", Atespace: "team-b"}); ok {
		t.Error("team-b was allowed")
	}
}
