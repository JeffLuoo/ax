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

package auth

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Rule is the RBAC check for one RPC.
type Rule struct {
	Verb, Resource string
	// atespace returns the request's atespace. An empty result on a list
	// means all atespaces, which only a cluster-wide grant allows.
	atespace func(req any) string
}

// orDefault is the atespace that the server uses when a request names none.
func orDefault(s string) string {
	if s == "" {
		return "default"
	}
	return s
}

func metaAtespace(m *v1alpha1.ObjectMeta) string {
	return orDefault(m.GetAtespace())
}

// Rules maps each AX RPC to its verb and resource. An RPC that is not in the
// map is denied.
var Rules = map[string]Rule{
	v1alpha1.AX_GetTask_FullMethodName:     {"get", "tasks", func(r any) string { return orDefault(r.(*v1alpha1.GetTaskRequest).GetAtespace()) }},
	v1alpha1.AX_ListTasks_FullMethodName:   {"list", "tasks", func(r any) string { return r.(*v1alpha1.ListTasksRequest).GetAtespace() }},
	v1alpha1.AX_CreateTask_FullMethodName:  {"create", "tasks", func(r any) string { return metaAtespace(r.(*v1alpha1.CreateTaskRequest).GetTask().GetMetadata()) }},
	v1alpha1.AX_DeleteTask_FullMethodName:  {"delete", "tasks", func(r any) string { return orDefault(r.(*v1alpha1.DeleteTaskRequest).GetAtespace()) }},
	v1alpha1.AX_SuspendTask_FullMethodName: {"update", "tasks", func(r any) string { return orDefault(r.(*v1alpha1.SuspendTaskRequest).GetAtespace()) }},
	v1alpha1.AX_ResumeTask_FullMethodName:  {"update", "tasks", func(r any) string { return orDefault(r.(*v1alpha1.ResumeTaskRequest).GetAtespace()) }},
	v1alpha1.AX_WatchTask_FullMethodName:   {"watch", "tasks", func(r any) string { return orDefault(r.(*v1alpha1.WatchTaskRequest).GetAtespace()) }},
	v1alpha1.AX_UpdateTaskBudget_FullMethodName: {"update", "tasks", func(r any) string {
		return orDefault(r.(*v1alpha1.UpdateTaskBudgetRequest).GetAtespace())
	}},

	v1alpha1.AX_GetWorkspace_FullMethodName:   {"get", "workspaces", func(r any) string { return orDefault(r.(*v1alpha1.GetWorkspaceRequest).GetAtespace()) }},
	v1alpha1.AX_ListWorkspaces_FullMethodName: {"list", "workspaces", func(r any) string { return r.(*v1alpha1.ListWorkspacesRequest).GetAtespace() }},
	v1alpha1.AX_UpdateWorkspace_FullMethodName: {"update", "workspaces", func(r any) string {
		return metaAtespace(r.(*v1alpha1.UpdateWorkspaceRequest).GetWorkspace().GetMetadata())
	}},
	v1alpha1.AX_DeleteWorkspace_FullMethodName: {"delete", "workspaces", func(r any) string { return orDefault(r.(*v1alpha1.DeleteWorkspaceRequest).GetAtespace()) }},

	v1alpha1.AX_GetModel_FullMethodName:    {"get", "models", func(r any) string { return orDefault(r.(*v1alpha1.GetModelRequest).GetAtespace()) }},
	v1alpha1.AX_ListModels_FullMethodName:  {"list", "models", func(r any) string { return r.(*v1alpha1.ListModelsRequest).GetAtespace() }},
	v1alpha1.AX_UpdateModel_FullMethodName: {"update", "models", func(r any) string { return metaAtespace(r.(*v1alpha1.UpdateModelRequest).GetModel().GetMetadata()) }},
	v1alpha1.AX_DeleteModel_FullMethodName: {"delete", "models", func(r any) string { return orDefault(r.(*v1alpha1.DeleteModelRequest).GetAtespace()) }},
}

// Guard authenticates and authorizes every AX RPC.
type Guard struct {
	Authn Authenticator
	Authz Authorizer
}

// bearerToken returns the token in the request's authorization header.
func bearerToken(ctx context.Context) string {
	md, _ := metadata.FromIncomingContext(ctx)
	for _, v := range md.Get("authorization") {
		if scheme, tok, ok := strings.Cut(v, " "); ok && strings.EqualFold(scheme, "bearer") {
			return strings.TrimSpace(tok)
		}
	}
	return ""
}

// authenticate verifies the caller's token and returns ctx with the caller.
func (g *Guard) authenticate(ctx context.Context, method string) (context.Context, error) {
	if _, ok := Rules[method]; !ok {
		return nil, status.Errorf(codes.PermissionDenied, "method %s is not allowed", method)
	}
	p, err := g.Authn.Authenticate(ctx, bearerToken(ctx))
	if errors.Is(err, ErrUnauthenticated) {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	if err != nil {
		slog.Error("authenticating caller", "method", method, "error", err)
		return nil, status.Error(codes.Unavailable, "cannot verify the caller's token")
	}
	return WithPrincipal(ctx, p), nil
}

// authorize checks that the caller in ctx may make req to method.
func (g *Guard) authorize(ctx context.Context, method string, req any) error {
	rule := Rules[method]
	p, _ := PrincipalFrom(ctx)
	attrs := Attributes{Verb: rule.Verb, Resource: rule.Resource, Atespace: rule.atespace(req)}
	ok, reason, err := g.Authz.Authorize(ctx, p, attrs)
	if err != nil {
		slog.Error("authorizing caller", "method", method, "user", p.Username, "error", err)
		return status.Error(codes.Unavailable, "cannot check the caller's access")
	}
	if !ok {
		scope := "atespace " + attrs.Atespace
		if attrs.Atespace == "" {
			scope = "all atespaces"
		}
		msg := p.Username + " cannot " + attrs.Verb + " " + Group + "/" + attrs.Resource + " in " + scope
		if reason != "" {
			msg += ": " + reason
		}
		return status.Error(codes.PermissionDenied, msg)
	}
	return nil
}

// Unary is the unary server interceptor.
func (g *Guard) Unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ctx, err := g.authenticate(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}
	if err := g.authorize(ctx, info.FullMethod, req); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// Stream is the stream server interceptor. AX streams are server streams with
// one request, so the stream authorizes that request when the handler reads
// it, before the handler can act on it.
func (g *Guard) Stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx, err := g.authenticate(ss.Context(), info.FullMethod)
	if err != nil {
		return err
	}
	return handler(srv, &guardedStream{ServerStream: ss, ctx: ctx, guard: g, method: info.FullMethod})
}

type guardedStream struct {
	grpc.ServerStream
	ctx        context.Context
	guard      *Guard
	method     string
	authorized bool
}

func (s *guardedStream) Context() context.Context { return s.ctx }

func (s *guardedStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if s.authorized {
		return status.Error(codes.InvalidArgument, "a stream takes one request")
	}
	if err := s.guard.authorize(s.ctx, s.method, m); err != nil {
		return err
	}
	s.authorized = true
	return nil
}

func (s *guardedStream) SendMsg(m any) error {
	if !s.authorized {
		return status.Error(codes.PermissionDenied, "the request is not authorized")
	}
	return s.ServerStream.SendMsg(m)
}
