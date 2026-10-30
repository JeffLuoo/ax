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

// Package runner implements the logic that runs inside every AX task container.
//
// Given a Task and its Workspace, a run starts the metadata and guest server,
// performs the maiden-run workspace setup, and then starts the task's command
// as a supervised child process. The runner stays up as the container's main
// process until it is told to stop, so the sandbox remains inspectable after
// the command has finished. The ax-task-runner binary is a thin wrapper around
// [Run]; custom images can embed the same behavior by calling it directly.
package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/google/ax/internal/metadata"
	"github.com/google/ax/internal/workspace"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

const (
	// DefaultPort is where the probe and guest server listens when Config.Port
	// is zero. It matches the port Agent Substrate probes for readiness.
	DefaultPort = 80
	// DefaultMetadataPort is the loopback port for the metadata endpoints when
	// Config.MetadataPort is zero.
	DefaultMetadataPort = 8099
	// DefaultWorkspacePath is the command's working directory unless the task's
	// workspace binding sets a path.
	DefaultWorkspacePath = v1alpha1.DefaultWorkspacePath

	// DefaultStateDir is where the runner keeps state that must survive a
	// resume when Config.StateDir is empty. Agent Substrate restores only the
	// durable workspace volume into the new container, so it lives there.
	DefaultStateDir = v1alpha1.DefaultWorkspacePath + "/.ax"
	// ExitRecordFile, under the state directory, holds the task command's
	// exit once the command has exited on its own. The task command can write
	// this file too, so the runner checks it when it reads it.
	ExitRecordFile = "command-exit.json"

	// maxExitRecordSkew is how far in the future the finish time of an exit
	// record can be. A resume can move the sandbox to a node whose clock is
	// a little behind.
	maxExitRecordSkew = time.Minute

	// stopGracePeriod is how long a running task command gets to exit after
	// SIGTERM before it is killed during shutdown.
	stopGracePeriod = 10 * time.Second
)

// ExitRecord is the task command's exit, as the runner keeps it in the state
// directory.
type ExitRecord struct {
	ExitCode   int       `json:"exitCode"`
	FinishedAt time.Time `json:"finishedAt"`
}

// readExitRecord returns the exit record in dir, or nil when there is none.
func readExitRecord(dir string) (*ExitRecord, error) {
	data, err := os.ReadFile(filepath.Join(dir, ExitRecordFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rec ExitRecord
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", ExitRecordFile, err)
	}
	if dec.More() {
		return nil, fmt.Errorf("parsing %s: data after the record", ExitRecordFile)
	}
	if err := rec.validate(time.Now()); err != nil {
		return nil, fmt.Errorf("checking %s: %w", ExitRecordFile, err)
	}
	return &rec, nil
}

// validate rejects a record that the runner cannot have written. The record
// is in the workspace volume, so the agent or a repository that it clones can
// write it. The check stops a damaged record, not an agent that writes a
// plausible one: the outcome is what the sandbox reports.
func (r ExitRecord) validate(now time.Time) error {
	// ProcessState.ExitCode is -1 for a command that a signal stopped, and
	// 0 to 255 for a command that exited.
	if r.ExitCode < -1 || r.ExitCode > 255 {
		return fmt.Errorf("exit code %d is not in the range -1 to 255", r.ExitCode)
	}
	if r.FinishedAt.IsZero() {
		return errors.New("no finish time")
	}
	if r.FinishedAt.After(now.Add(maxExitRecordSkew)) {
		return fmt.Errorf("finish time %s is in the future", r.FinishedAt.Format(time.RFC3339))
	}
	return nil
}

// writeExitRecord stores rec in dir. It writes a temporary file, syncs it, and
// renames it, so a snapshot never captures a partial record.
func writeExitRecord(dir string, rec ExitRecord) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ExitRecordFile+".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err := f.Write(data); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, ExitRecordFile))
}

// Config controls a single Run.
type Config struct {
	// Port is where the probe endpoints and guest gRPC services listen.
	// Zero means DefaultPort.
	Port int
	// MetadataPort is the loopback-only port for the metadata endpoints the task
	// command reads through AX_METADATA_URL. Zero means DefaultMetadataPort.
	MetadataPort int
	// Task is the task to run. It supplies the command, its environment, and
	// the workspace binding. Nil runs with no command and default settings.
	Task *v1alpha1.Task
	// Workspace is the Workspace resource the task's spec.workspace refers to.
	// Nil sets up an empty working directory instead.
	Workspace *v1alpha1.Workspace
	// OnCommandExit, when set, is called once the task command has exited.
	OnCommandExit func(CommandExit)
	// StateDir is where the runner keeps the command's exit record so that it
	// survives a resume. Empty means DefaultStateDir.
	StateDir string
}

// CommandExit describes how the task command finished.
type CommandExit struct {
	// Pid of the command's process.
	Pid int
	// ExitCode is the command's exit status, or -1 if it was killed by a signal.
	ExitCode int
	// Err is nil when the command exited with status zero.
	Err error
}

// Run executes the task-runner lifecycle and blocks until ctx is cancelled.
//
// The task's workspace is set up at its path, /workspace unless the binding
// says otherwise. The task's spec.command, if any, is then started as a child
// process in its own process group with that path as its working directory and
// AX_METADATA_URL plus spec.env in its environment. The metadata and guest
// server keeps serving whether or not the command is still running. When ctx is
// cancelled a running command is sent SIGTERM, given a grace period, and then
// killed.
//
// When the command exits on its own, Run writes an exit record to the state
// directory and serves it at GET /statusz. A later Run on the same volume, for
// example after a resume, finds the record and serves it again: it does not
// set up the workspace or start the command a second time. A command that Run
// stops because ctx is cancelled (a suspend) gets no record, so it starts again
// on the next Run.
//
// Workspace setup failures are logged but do not abort the run; the task simply
// never reports ready. Run returns an error only when the runner itself cannot
// start, for example when the metadata server or the command fails to launch.
func Run(ctx context.Context, cfg Config) error {
	port := cfg.Port
	if port <= 0 {
		port = DefaultPort
	}
	metadataPort := cfg.MetadataPort
	if metadataPort <= 0 {
		metadataPort = DefaultMetadataPort
	}
	ref := cfg.Task.GetSpec().GetWorkspace()
	wsPath := cfg.Task.GetSpec().WorkspacePath()
	slog.Info("initializing AX task runner", "port", port, "workspace", ref.GetName(), "wsPath", wsPath)

	metaServer := metadata.NewServer(port, cfg.Task, cfg.Workspace, metadata.ServerOptions{
		WorkspacePath: wsPath,
		MetadataPort:  metadataPort,
	})
	if err := metaServer.Start(); err != nil {
		return fmt.Errorf("starting metadata server: %w", err)
	}
	metadataURL := metaServer.MetadataURL()
	_ = os.Setenv("AX_METADATA_URL", metadataURL)
	for _, e := range cfg.Task.GetSpec().GetEnv() {
		_ = os.Setenv(e.GetName(), e.GetValue())
	}
	defer func() {
		slog.Info("shutting down metadata server")
		_ = metaServer.Stop(context.Background())
	}()

	stateDir := cfg.StateDir
	if stateDir == "" {
		stateDir = DefaultStateDir
	}
	prev, err := readExitRecord(stateDir)
	if err != nil {
		// An unreadable or invalid record is not proof that the command
		// ran, so ignore it and run the command.
		slog.Error("ignoring the task command's exit record; starting the command", "dir", stateDir, "error", err)
	}
	if prev != nil {
		metaServer.SetWorkspaceReady(true)
		metaServer.SetCommandExited(prev.ExitCode, prev.FinishedAt)
		slog.Info("task command already exited; not starting it again", "exitCode", prev.ExitCode, "finishedAt", prev.FinishedAt)
		<-ctx.Done()
		return nil
	}

	if _, err := workspace.SetupIn(ctx, cfg.Workspace, wsPath, stateDir); err != nil {
		slog.Error("workspace maiden run setup failed", "workspace", ref.GetName(), "path", wsPath, "error", err)
	} else {
		metaServer.SetWorkspaceReady(true)
		slog.Info("workspace maiden run setup marked ready", "path", wsPath)
	}

	cmdArgs := cfg.Task.GetSpec().GetCommand()
	if len(cmdArgs) == 0 {
		slog.Info("no task command specified; serving metadata until stopped")
		<-ctx.Done()
		return nil
	}

	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	cmd.Dir = wsPath
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = commandEnv(cfg.Task, metadataURL)
	// A dedicated process group lets shutdown signal the command together with
	// anything it spawned.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting task command %v: %w", cmdArgs, err)
	}
	metaServer.SetCommandRunning()
	slog.Info("started task command", "pid", cmd.Process.Pid, "command", cmdArgs)

	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()

	select {
	case err := <-exited:
		rec := ExitRecord{ExitCode: -1, FinishedAt: time.Now().UTC()}
		if cmd.ProcessState != nil {
			rec.ExitCode = cmd.ProcessState.ExitCode()
		}
		if werr := writeExitRecord(stateDir, rec); werr != nil {
			slog.Error("writing the task command's exit record; a resume will start the command again", "dir", stateDir, "error", werr)
		}
		metaServer.SetCommandExited(rec.ExitCode, rec.FinishedAt)
		reportExit(cfg, cmd, err)
		// Keep the sandbox up and inspectable until told to stop.
		<-ctx.Done()
	case <-ctx.Done():
		reportExit(cfg, cmd, stopCommand(cmd, exited))
	}
	return nil
}

// stopCommand asks the command's process group to terminate, kills it if it has
// not exited within the grace period, and returns the command's exit result.
func stopCommand(cmd *exec.Cmd, exited <-chan error) error {
	pid := cmd.Process.Pid
	slog.Info("stopping task command", "pid", pid)
	_ = syscall.Kill(-pid, syscall.SIGTERM)

	timer := time.NewTimer(stopGracePeriod)
	defer timer.Stop()
	select {
	case err := <-exited:
		return err
	case <-timer.C:
		slog.Warn("task command did not exit within grace period; killing", "pid", pid, "gracePeriod", stopGracePeriod)
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		return <-exited
	}
}

// reportExit logs how the command finished and notifies the OnCommandExit hook.
func reportExit(cfg Config, cmd *exec.Cmd, err error) {
	exit := CommandExit{Pid: cmd.Process.Pid, ExitCode: -1, Err: err}
	if cmd.ProcessState != nil {
		exit.ExitCode = cmd.ProcessState.ExitCode()
	}
	if err != nil {
		slog.Error("task command exited with error", "pid", exit.Pid, "exitCode", exit.ExitCode, "error", err)
	} else {
		slog.Info("task command completed successfully", "pid", exit.Pid)
	}
	if cfg.OnCommandExit != nil {
		cfg.OnCommandExit(exit)
	}
}

// commandEnv builds the environment for the task command: the runner's own
// environment, the metadata URL, and the Task's spec.env entries.
func commandEnv(task *v1alpha1.Task, metadataURL string) []string {
	env := append(os.Environ(), "AX_METADATA_URL="+metadataURL)
	for _, e := range task.GetSpec().GetEnv() {
		env = append(env, fmt.Sprintf("%s=%s", e.GetName(), e.GetValue()))
	}
	return env
}
