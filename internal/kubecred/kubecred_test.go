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

package kubecred_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/google/ax/internal/kubecred"
)

// writeKubeconfig writes a kubeconfig with one context per user entry and
// points $KUBECONFIG at it.
func writeKubeconfig(t *testing.T, users string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config")
	cfg := `apiVersion: v1
kind: Config
current-context: static
clusters:
- name: c
  cluster:
    server: https://127.0.0.1:1
contexts:
- name: static
  context: {cluster: c, user: static}
- name: file
  context: {cluster: c, user: file}
- name: exec
  context: {cluster: c, user: exec}
- name: cert
  context: {cluster: c, user: cert}
users:
` + users
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", path)
	t.Setenv("KUBECONTEXT", "")
	t.Setenv(kubecred.TokenEnv, "")
}

func TestSource_Token(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the exec plugin is a shell script")
	}
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("file-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	plugin := filepath.Join(dir, "plugin.sh")
	script := `#!/bin/sh
echo '{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","status":{"token":"exec-token"}}'
`
	if err := os.WriteFile(plugin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	writeKubeconfig(t, `- name: static
  user: {token: static-token}
- name: file
  user: {tokenFile: `+tokenFile+`}
- name: exec
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: `+plugin+`
      interactiveMode: Never
- name: cert
  user: {username: u, password: p}
`)

	for _, tc := range []struct{ context, want string }{
		{"", "static-token"},
		{"static", "static-token"},
		{"file", "file-token"},
		{"exec", "exec-token"},
		{"cert", ""},
	} {
		got, err := kubecred.NewSource(tc.context).Token(context.Background())
		if err != nil || got != tc.want {
			t.Errorf("context %q: Token() = %q, %v, want %q", tc.context, got, err, tc.want)
		}
	}

	t.Setenv("KUBECONTEXT", "file")
	if got, _ := kubecred.NewSource("").Token(context.Background()); got != "file-token" {
		t.Errorf("$KUBECONTEXT=file: Token() = %q, want file-token", got)
	}

	t.Setenv(kubecred.TokenEnv, "env-token")
	if got, _ := kubecred.NewSource("exec").Token(context.Background()); got != "env-token" {
		t.Errorf("$%s: Token() = %q, want env-token", kubecred.TokenEnv, got)
	}
}

func TestSource_PerRPC(t *testing.T) {
	writeKubeconfig(t, `- name: static
  user: {token: static-token}
`)
	md, err := kubecred.PerRPC(kubecred.NewSource("")).GetRequestMetadata(context.Background())
	if err != nil || md["authorization"] != "Bearer static-token" {
		t.Fatalf("metadata = %v, %v", md, err)
	}
	if kubecred.PerRPC(nil).RequireTransportSecurity() {
		t.Error("RequireTransportSecurity = true, want false for the port-forward")
	}
}

func TestSource_NoKubeconfig(t *testing.T) {
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("KUBECONTEXT", "")
	t.Setenv(kubecred.TokenEnv, "")
	t.Setenv("HOME", t.TempDir())
	got, err := kubecred.NewSource("").Token(context.Background())
	if err != nil || got != "" {
		t.Fatalf("Token() = %q, %v, want no token and no error", got, err)
	}
	if _, err := kubecred.NewSource("absent").Token(context.Background()); err == nil {
		t.Error("a named context in no kubeconfig: want an error")
	}
}
