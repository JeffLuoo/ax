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

// Package auth authenticates and authorizes ax-server's callers with the
// Kubernetes API. A caller sends a bearer token. A TokenReview verifies it and
// names the caller, and a SubjectAccessReview checks the RPC against RBAC on a
// virtual resource in API group ax.io, with the request's atespace as the
// namespace. ax-server acts on Substrate with its own credentials for every
// atespace, so without this check any caller would get those rights.
package auth

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	authzv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Principal is a caller that a TokenReview verified.
type Principal struct {
	Username string
	UID      string
	Groups   []string
	Extra    map[string][]string
}

type principalKey struct{}

// WithPrincipal returns ctx with the verified caller.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFrom returns the verified caller in ctx, if there is one.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// ErrUnauthenticated means that the token is missing or that the Kubernetes
// API rejected it. Any other error from Authenticate is a failed check, not a
// rejection.
var ErrUnauthenticated = errors.New("unauthenticated")

// Authenticator verifies a bearer token.
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (Principal, error)
}

// Defaults for TokenReviewAuthenticator.
const (
	DefaultAudience   = "ax-server"
	DefaultCacheTTL   = time.Minute
	DefaultMaxEntries = 10000
)

// TokenReviewAuthenticator verifies tokens with the TokenReview API.
type TokenReviewAuthenticator struct {
	client kubernetes.Interface
	// audiences are the audiences a token must be for.
	audiences []string
	// allowAPIServerTokens also accepts tokens for the Kubernetes API
	// server's own audiences when a token is not for audiences.
	allowAPIServerTokens bool
	ttl                  time.Duration
	maxEntries           int
	now                  func() time.Time

	mu    sync.Mutex
	cache map[[sha256.Size]byte]cacheEntry
}

type cacheEntry struct {
	p       Principal
	err     error
	expires time.Time
}

// TokenReviewOptions configures a TokenReviewAuthenticator.
type TokenReviewOptions struct {
	// Audiences a token must be for; nil means DefaultAudience.
	Audiences []string
	// AllowAPIServerTokens also accepts a token for the Kubernetes API
	// server's own audiences, such as the credential in a user's
	// kubeconfig. Such a token is not bound to ax-server, so ax-server could
	// use it against the Kubernetes API; turn it on only for callers who
	// authenticate with their kubeconfig.
	AllowAPIServerTokens bool
	// CacheTTL is how long a review result is kept; 0 means DefaultCacheTTL.
	CacheTTL time.Duration
	// MaxEntries bounds the cache; 0 means DefaultMaxEntries.
	MaxEntries int
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// NewTokenReviewAuthenticator returns an authenticator that calls client.
func NewTokenReviewAuthenticator(client kubernetes.Interface, opts TokenReviewOptions) *TokenReviewAuthenticator {
	a := &TokenReviewAuthenticator{client: client, audiences: opts.Audiences, allowAPIServerTokens: opts.AllowAPIServerTokens,
		ttl: opts.CacheTTL, maxEntries: opts.MaxEntries, now: opts.Now, cache: map[[sha256.Size]byte]cacheEntry{}}
	if a.audiences == nil {
		a.audiences = []string{DefaultAudience}
	}
	if a.ttl <= 0 {
		a.ttl = DefaultCacheTTL
	}
	if a.maxEntries <= 0 {
		a.maxEntries = DefaultMaxEntries
	}
	if a.now == nil {
		a.now = time.Now
	}
	return a
}

// errRejected marks a token that the Kubernetes API did not authenticate.
var errRejected = fmt.Errorf("%w: the Kubernetes API rejected the token", ErrUnauthenticated)

// Authenticate implements Authenticator. It caches a success or a rejection
// for the cache TTL, keyed by the token's SHA-256, and caches no API error, so
// a failed check is tried again and never admits the caller.
func (a *TokenReviewAuthenticator) Authenticate(ctx context.Context, token string) (Principal, error) {
	if token == "" {
		return Principal{}, fmt.Errorf("%w: no bearer token", ErrUnauthenticated)
	}
	key := sha256.Sum256([]byte(token))
	now := a.now()
	a.mu.Lock()
	if e, ok := a.cache[key]; ok && now.Before(e.expires) {
		a.mu.Unlock()
		return e.p, e.err
	}
	a.mu.Unlock()

	p, err := a.review(ctx, token, a.audiences)
	if errors.Is(err, errRejected) && a.allowAPIServerTokens {
		p, err = a.review(ctx, token, nil)
	}
	if err != nil && !errors.Is(err, errRejected) {
		return Principal{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.cache) >= a.maxEntries {
		for k, e := range a.cache {
			if !now.Before(e.expires) {
				delete(a.cache, k)
			}
		}
		for k := range a.cache {
			if len(a.cache) < a.maxEntries {
				break
			}
			delete(a.cache, k)
		}
	}
	a.cache[key] = cacheEntry{p: p, err: err, expires: now.Add(a.ttl)}
	return p, err
}

// review sends one TokenReview. With no audiences, the API server checks the
// token against its own audiences.
func (a *TokenReviewAuthenticator) review(ctx context.Context, token string, audiences []string) (Principal, error) {
	tr := &authnv1.TokenReview{Spec: authnv1.TokenReviewSpec{Token: token, Audiences: audiences}}
	res, err := a.client.AuthenticationV1().TokenReviews().Create(ctx, tr, metav1.CreateOptions{})
	if err != nil {
		return Principal{}, fmt.Errorf("token review: %w", err)
	}
	st := res.Status
	if !st.Authenticated || st.User.Username == "" {
		return Principal{}, errRejected
	}
	if len(audiences) > 0 && !slices.ContainsFunc(st.Audiences, func(s string) bool { return slices.Contains(audiences, s) }) {
		return Principal{}, errRejected
	}
	extra := map[string][]string{}
	for k, v := range st.User.Extra {
		extra[k] = v
	}
	return Principal{Username: st.User.Username, UID: st.User.UID, Groups: st.User.Groups, Extra: extra}, nil
}

// Attributes are what one RPC asks to do.
type Attributes struct {
	// Verb and Resource are the RBAC verb and resource in API group Group.
	Verb, Resource string
	// Atespace is the request's atespace, checked as the namespace.
	Atespace string
}

// Group is the API group of the virtual resources that RBAC rules name.
const Group = "ax.io"

// Authorizer decides whether a caller may do what an RPC asks.
type Authorizer interface {
	// Authorize returns whether p may act, and the reason the API gave.
	Authorize(ctx context.Context, p Principal, attrs Attributes) (bool, string, error)
}

// SubjectAccessReviewAuthorizer asks the Kubernetes API with a
// SubjectAccessReview, so RBAC rules grant ax-server's verbs.
type SubjectAccessReviewAuthorizer struct {
	client kubernetes.Interface
}

// NewSubjectAccessReviewAuthorizer returns an authorizer that calls client.
func NewSubjectAccessReviewAuthorizer(client kubernetes.Interface) *SubjectAccessReviewAuthorizer {
	return &SubjectAccessReviewAuthorizer{client: client}
}

// Authorize implements Authorizer.
func (z *SubjectAccessReviewAuthorizer) Authorize(ctx context.Context, p Principal, attrs Attributes) (bool, string, error) {
	extra := map[string]authzv1.ExtraValue{}
	for k, v := range p.Extra {
		extra[k] = v
	}
	sar := &authzv1.SubjectAccessReview{Spec: authzv1.SubjectAccessReviewSpec{
		User: p.Username, UID: p.UID, Groups: p.Groups, Extra: extra,
		ResourceAttributes: &authzv1.ResourceAttributes{Group: Group, Resource: attrs.Resource, Verb: attrs.Verb, Namespace: attrs.Atespace},
	}}
	res, err := z.client.AuthorizationV1().SubjectAccessReviews().Create(ctx, sar, metav1.CreateOptions{})
	if err != nil {
		return false, "", fmt.Errorf("subject access review: %w", err)
	}
	return res.Status.Allowed, res.Status.Reason, nil
}
