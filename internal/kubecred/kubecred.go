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

// Package kubecred gives the ax CLI a bearer token for ax-server. ax-server
// verifies the token with a Kubernetes TokenReview, so the CLI sends the
// credential of the kubeconfig context: a static token, a token file, or the
// token of an exec plugin such as gke-gcloud-auth-plugin.
package kubecred

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"google.golang.org/grpc/credentials"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// TokenEnv names an environment variable whose token the CLI sends instead of
// the kubeconfig credential, for example a service account token from
// "kubectl create token --audience ax-server".
const TokenEnv = "AX_TOKEN"

// Source returns the current bearer token of one kubeconfig context.
type Source struct {
	once sync.Once
	rt   http.RoundTripper
	host string
	err  error

	loadingRules *clientcmd.ClientConfigLoadingRules
	context      string
}

// NewSource returns a Source for kubeContext. "" means $KUBECONTEXT, and then
// the current context, the same order as the CLI's port-forward uses.
func NewSource(kubeContext string) *Source {
	if kubeContext == "" {
		kubeContext = os.Getenv("KUBECONTEXT")
	}
	return &Source{loadingRules: clientcmd.NewDefaultClientConfigLoadingRules(), context: kubeContext}
}

// errCaptured stops the request after the credential wrappers set its header.
var errCaptured = errors.New("captured")

type capture struct{ header string }

func (c *capture) RoundTrip(r *http.Request) (*http.Response, error) {
	c.header = r.Header.Get("Authorization")
	return nil, errCaptured
}

// Token returns the bearer token, or "" when the context has no token, for
// example when it authenticates with a client certificate.
func (s *Source) Token(ctx context.Context) (string, error) {
	if tok := strings.TrimSpace(os.Getenv(TokenEnv)); tok != "" {
		return tok, nil
	}
	s.once.Do(func() {
		cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(s.loadingRules,
			&clientcmd.ConfigOverrides{CurrentContext: s.context}).ClientConfig()
		if clientcmd.IsEmptyConfig(err) {
			// No kubeconfig: send no token. ax-server with --auth=false
			// accepts that; with auth on, it returns Unauthenticated.
			return
		}
		if err != nil {
			s.err = fmt.Errorf("loading kubeconfig: %w", err)
			return
		}
		s.host = cfg.Host
		// client-go adds the token to each request in a wrapper of the
		// transport. The wrappers run a token file read or an exec plugin and
		// cache the result, so the CLI uses them, not its own copy of that
		// logic.
		s.rt, s.err = rest.HTTPWrappersForConfig(cfg, &passthrough{})
	})
	if s.err != nil || s.rt == nil {
		return "", s.err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.host, nil)
	if err != nil {
		return "", fmt.Errorf("kubeconfig server %q: %w", s.host, err)
	}
	c := &capture{}
	req = req.WithContext(context.WithValue(ctx, captureKey{}, c))
	if _, err := s.rt.RoundTrip(req); err != nil && !errors.Is(err, errCaptured) {
		return "", fmt.Errorf("getting the kubeconfig credential: %w", err)
	}
	scheme, tok, ok := strings.Cut(c.header, " ")
	if !ok || !strings.EqualFold(scheme, "bearer") {
		return "", nil
	}
	return tok, nil
}

type captureKey struct{}

// passthrough hands each request to the capture in its context, so one
// wrapped transport serves calls from many goroutines.
type passthrough struct{}

func (passthrough) RoundTrip(r *http.Request) (*http.Response, error) {
	c, ok := r.Context().Value(captureKey{}).(*capture)
	if !ok {
		return nil, errors.New("kubecred: no capture in the request context")
	}
	return c.RoundTrip(r)
}

// PerRPC returns gRPC per-RPC credentials that send the token of src. They
// also work on a plaintext connection, because the CLI reaches ax-server
// through a kubectl port-forward on localhost.
func PerRPC(src *Source) credentials.PerRPCCredentials { return perRPC{src} }

type perRPC struct{ src *Source }

func (p perRPC) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	tok, err := p.src.Token(ctx)
	if err != nil {
		return nil, err
	}
	if tok == "" {
		return nil, nil
	}
	return map[string]string{"authorization": "Bearer " + tok}, nil
}

func (perRPC) RequireTransportSecurity() bool { return false }
