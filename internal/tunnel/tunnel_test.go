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

package tunnel

import (
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestSanitizeContext(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"my-cluster", "my-cluster"},
		{"gke_project_us-central1_cluster", "gke_project_us-central1_cluster"},
		{"arn:aws:eks:us-west-2:123456789:cluster/prod", "arn_aws_eks_us-west-2_123456789_cluster_prod"},
		{"", "default"},
	}

	for _, tt := range tests {
		got := SanitizeContext(tt.input)
		if got != tt.want {
			t.Errorf("SanitizeContext(%q) = %q; want %q", tt.input, got, tt.want)
		}
	}
}

func TestPortForwardRegex(t *testing.T) {
	cases := []struct {
		log  string
		port string
	}{
		{"Forwarding from 127.0.0.1:54947 -> 8080\n", "54947"},
		{"Forwarding from [::1]:61234 -> 8080\n", "61234"},
		{"Some prelude\nForwarding from 127.0.0.1:8080 -> 8080\n", "8080"},
	}

	for _, c := range cases {
		match := portForwardRegex.FindStringSubmatch(c.log)
		if len(match) < 2 || match[1] != c.port {
			t.Errorf("expected port %q from %q, got %v", c.port, c.log, match)
		}
	}
}

func healthOnlyServer(t *testing.T, path string) int {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == path {
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	_, portStr, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}
	return port
}

func TestIsCostTunnelHealthy_ProbesHealthz(t *testing.T) {
	if !IsCostTunnelHealthy(healthOnlyServer(t, "/healthz")) {
		t.Error("a server that answers /healthz is not healthy")
	}
	// /v1/costs builds a full report; it is not the health check.
	if IsCostTunnelHealthy(healthOnlyServer(t, "/v1/costs")) {
		t.Error("a server without /healthz counts as healthy")
	}
}

func TestEnsureCostURL_UsesOnlyTheContextsTunnel(t *testing.T) {
	t.Setenv("AX_HOME", t.TempDir())
	t.Setenv("AX_COST_SERVER", "")
	t.Setenv("AX_FINOPS_SERVER", "")
	t.Setenv("KUBECONTEXT", "")
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "none"))
	// No kubectl: a new port-forward cannot start.
	t.Setenv("PATH", t.TempDir())

	if _, err := EnsureCostURL(Options{}); err == nil {
		t.Error("EnsureCostURL without a kube context did not fail")
	}

	port := healthOnlyServer(t, "/healthz")
	if err := SaveTunnel(&TunnelInfo{Context: CostTunnelKey("staging"), Namespace: "ate-cost", Service: "cost", Port: port}); err != nil {
		t.Fatal(err)
	}
	got, err := EnsureCostURL(Options{Context: "staging"})
	if err != nil || got != "http://127.0.0.1:"+strconv.Itoa(port) {
		t.Errorf("EnsureCostURL(staging) = %q, %v; want the saved tunnel", got, err)
	}
	// prod has no saved tunnel; the healthy staging port must not be used.
	if got, err := EnsureCostURL(Options{Context: "prod"}); err == nil {
		t.Errorf("EnsureCostURL(prod) = %q, want an error (no tunnel for prod and no kubectl)", got)
	}
	if got, _ := EnsureCostURL(Options{Context: "prod", ServerURL: "http://explicit:1"}); got != "http://explicit:1" {
		t.Errorf("explicit server = %q", got)
	}
}
