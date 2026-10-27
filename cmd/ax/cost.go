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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/google/ax/internal/costapi"
	"github.com/google/ax/internal/tunnel"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/yaml.v3"
)

const costUsage = `Usage:
  ax cost [tasks|actors] [-a <atespace> | -A] [-o table|wide|json|yaml]
  ax cost task <name> [-a <atespace>] [-o table|json|yaml]
  ax cost actor <name> [-a <atespace>] [-o table|json|yaml]
  ax cost curve [-o table|json|yaml]
  ax cost focus [-o table|json|csv|yaml]
  ax cost budgets [-a <atespace>] [-o table|json|yaml]

Query Cost Service attribution, active hosting cost breakdown
(booked-share vs. measured-usage), per-session spend, task budgets and
outcomes, the < $1/agent-month hosting sensitivity curve, FOCUS 1.3
allocation keys (usage per agent and origin resource; the holder of the bill
writes the split cost rows), and budget states (OK, WARN, EXHAUSTED) with the
actors that the Cost Service holds.

-a limits the query to one atespace, on the server; without -a, the default
atespace is shown, or all atespaces when it is empty. A task or actor is a
name, "<atespace>/<name>", an actor UID, or a unique UID prefix of at least
8 characters. -o json and -o yaml print the Cost Service's response as it
came. Numbers from rate card prices are labeled ESTIMATE — NOT RECONCILED.

Examples:
  ax cost -a otel-bench
  ax cost -A -o wide
  ax cost task coding-parent-1 -a otel-bench
  ax cost curve
  ax cost focus
  ax cost focus -o csv
  ax cost budgets`

type costOptions struct {
	subcmd       string // "list", "get", "curve", "focus", "budgets"
	target       string // task/actor name or UID when subcmd == "get"
	allAtespaces bool
	output       string // "table", "wide", "json", "yaml", "csv"
	help         bool
}

func parseCostArgs(args []string) (costOptions, error) {
	opts := costOptions{
		subcmd: "list",
		output: "table",
	}
	var positional []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-h" || arg == "--help" || arg == "help":
			opts.help = true
			return opts, nil
		case arg == "-A" || arg == "--all-atespaces" || arg == "--all-namespaces":
			opts.allAtespaces = true
		case arg == "-o" || arg == "--output":
			if i+1 >= len(args) {
				return opts, fmt.Errorf("%s requires an output format (table, wide, json, yaml, csv)", arg)
			}
			opts.output = strings.ToLower(strings.TrimSpace(args[i+1]))
			i++
		case strings.HasPrefix(arg, "-o="):
			opts.output = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(arg, "-o=")))
		case strings.HasPrefix(arg, "--output="):
			opts.output = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(arg, "--output=")))
		case strings.HasPrefix(arg, "-"):
			return opts, fmt.Errorf("unknown flag %q", arg)
		default:
			positional = append(positional, arg)
		}
	}

	switch opts.output {
	case "table", "wide", "json", "yaml", "csv":
	default:
		return opts, fmt.Errorf("unsupported output format %q (expected table, wide, json, yaml, or csv)", opts.output)
	}

	if len(positional) == 0 {
		return opts, nil
	}

	first := strings.ToLower(positional[0])
	switch first {
	case "curve", "curves":
		opts.subcmd = "curve"
	case "focus":
		opts.subcmd = "focus"
	case "budgets", "budget":
		opts.subcmd = "budgets"
	case "tasks", "actors":
		if len(positional) >= 2 {
			opts.subcmd = "get"
			opts.target = positional[1]
		} else {
			opts.subcmd = "list"
		}
	case "task", "actor", "agent":
		if len(positional) < 2 {
			return opts, fmt.Errorf("usage: ax cost %s <name>", first)
		}
		opts.subcmd = "get"
		opts.target = positional[1]
	default:
		opts.subcmd = "get"
		opts.target = positional[0]
	}

	return opts, nil
}

// runCost executes `ax cost ...` against the Cost Service HTTP API and joins task
// phases, budgets, and outcomes from the AX server when axServerURL is set.
// atespaceExplicit is true when the user passed -a; then nothing outside that
// atespace is shown or matched.
func runCost(costURL, axServerURL, atespace string, atespaceExplicit bool, args []string) error {
	opts, err := parseCostArgs(args)
	if err != nil {
		return err
	}
	if opts.help {
		fmt.Println(costUsage)
		return nil
	}
	out := os.Stdout

	client := costapi.NewClient(costURL, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	switch opts.subcmd {
	case "curve":
		curve, raw, err := client.GetCurve(ctx)
		if err != nil {
			return err
		}
		if isStructured(opts.output) {
			return writeRaw(out, raw, opts.output)
		}
		return renderCurveReport(out, curve)

	case "focus":
		scope := ""
		if atespaceExplicit && !opts.allAtespaces {
			scope = atespace
		}
		if opts.output == "csv" {
			csvData, err := client.GetFocusCSV(ctx, scope)
			if err != nil {
				return err
			}
			_, err = io.WriteString(out, csvData)
			return err
		}
		focus, raw, err := client.GetFocus(ctx, scope)
		if err != nil {
			return err
		}
		if isStructured(opts.output) {
			return writeRaw(out, raw, opts.output)
		}
		return renderFocusReport(out, focus)

	case "budgets":
		scope := ""
		if atespaceExplicit && !opts.allAtespaces {
			scope = atespace
		}
		report, raw, err := client.GetBudgets(ctx, scope)
		if err != nil {
			return err
		}
		if isStructured(opts.output) {
			return writeRaw(out, raw, opts.output)
		}
		return renderBudgets(out, report)

	case "get":
		m, task, err := resolveActorDetail(ctx, client, axServerURL, atespace, atespaceExplicit && !opts.allAtespaces, opts.target)
		if err != nil {
			return err
		}
		var budgets *costapi.BudgetsReport
		if !isStructured(opts.output) {
			budgets = fetchBudgets(ctx, client, m.Actor.Atespace)
		}
		return renderActorDetail(out, m, task, opts.output, budgets)

	default:
		scope := ""
		if !opts.allAtespaces {
			scope = atespace
		}
		report, raw, err := client.GetCosts(ctx, scope)
		if err != nil {
			return err
		}
		// Without -a, an empty default atespace shows every atespace.
		if !opts.allAtespaces && !atespaceExplicit && len(report.Actors) == 0 {
			scope = ""
			if report, raw, err = client.GetCosts(ctx, ""); err != nil {
				return err
			}
		}
		if isStructured(opts.output) {
			return writeRaw(out, raw, opts.output)
		}
		tasks := lookupAXTasks(ctx, axServerURL, scope)
		return renderCostList(out, report, scope, tasks, opts.output == "wide")
	}
}

func isStructured(format string) bool { return format == "json" || format == "yaml" }

// lookupAXTasks opportunistically lists the AX tasks, keyed by
// "<atespace>/<name>", when axServerURL is reachable. Errors are ignored so
// that `ax cost` works on clusters that run Substrate and the Cost Service without
// ax-server.
func lookupAXTasks(ctx context.Context, axServerURL, atespace string) map[string]*v1alpha1.Task {
	if axServerURL == "" {
		return nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	client, conn, err := getAXClient(axServerURL)
	if err != nil {
		return nil
	}
	defer conn.Close()
	const page = 500
	tasks := map[string]*v1alpha1.Task{}
	for offset := int64(0); ; offset += page {
		resp, err := client.ListTasks(qCtx, &v1alpha1.ListTasksRequest{Atespace: atespace, Limit: page, Offset: offset})
		if err != nil {
			if len(tasks) == 0 {
				return nil
			}
			return tasks
		}
		for _, t := range resp.Tasks {
			if t.GetMetadata() != nil {
				tasks[taskKey(t.GetMetadata().GetAtespace(), t.GetMetadata().GetName())] = t
			}
		}
		if len(resp.Tasks) < page {
			return tasks
		}
	}
}

func lookupAXTask(ctx context.Context, axServerURL, atespace, name string) *v1alpha1.Task {
	if axServerURL == "" {
		return nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	client, conn, err := getAXClient(axServerURL)
	if err != nil {
		return nil
	}
	defer conn.Close()
	t, err := client.GetTask(qCtx, &v1alpha1.GetTaskRequest{Atespace: atespace, Name: name})
	if err != nil {
		return nil
	}
	return t
}

func taskKey(atespace, name string) string {
	return firstNonEmpty(atespace, "default") + "/" + name
}

func addMoney(dst costapi.MoneyMap, srcs ...costapi.MoneyMap) {
	for _, src := range srcs {
		for k, v := range src {
			dst[k] += v
		}
	}
}

func roundMoney(m costapi.MoneyMap) costapi.MoneyMap {
	if len(m) == 0 {
		return nil
	}
	out := make(costapi.MoneyMap, len(m))
	for k, v := range m {
		out[k] = math.Round(v*1e12) / 1e12
	}
	return out
}

// buildTaskChildren builds a same-atespace parent -> sorted child keys map from
// Ax tasks. A child links to spec.parent only when the parent exists in the
// same atespace and status.parent_id matches the parent's status.id, so a
// recreated parent with a reused name does not inherit old children.
func buildTaskChildren(tasks map[string]*v1alpha1.Task) map[string][]string {
	if len(tasks) == 0 {
		return nil
	}
	children := map[string][]string{}
	for _, t := range tasks {
		if t.GetMetadata() == nil {
			continue
		}
		ns := firstNonEmpty(t.GetMetadata().GetAtespace(), "default")
		name := strings.TrimSpace(t.GetMetadata().GetName())
		parentName := strings.TrimSpace(t.GetSpec().GetParent())
		if name == "" || parentName == "" || parentName == name || strings.Contains(parentName, "/") {
			continue
		}
		parentKey := taskKey(ns, parentName)
		p := tasks[parentKey]
		if p == nil {
			continue
		}
		if t.GetStatus().GetParentId() != p.GetStatus().GetId() {
			continue
		}
		children[parentKey] = append(children[parentKey], taskKey(ns, name))
	}
	for k := range children {
		sort.Strings(children[k])
	}
	return children
}

// taskDescendants returns the keys ("<atespace>/<name>") of all descendants of
// rootKey in pre-order DFS, guarding against cycles.
func taskDescendants(children map[string][]string, rootKey string) []string {
	if len(children) == 0 {
		return nil
	}
	visited := map[string]bool{rootKey: true}
	var out []string
	var walk func(k string)
	walk = func(k string) {
		for _, c := range children[k] {
			if visited[c] {
				continue
			}
			visited[c] = true
			out = append(out, c)
			walk(c)
		}
	}
	walk(rootKey)
	return out
}

// rollupActorTree computes a's sub-agent count and tree totals from the Ax task
// tree and per-actor Cost Service costs, falling back to a zero ActorCost when the Cost Service
// lacks a cost record for a descendant task.
func rollupActorTree(a *costapi.ActorCost, isCanonical bool, actors []costapi.ActorCost, rawActors []json.RawMessage, children map[string][]string) ([]costapi.ActorCost, []json.RawMessage) {
	if a == nil || a.Note != "" {
		return nil, nil
	}
	llm := costapi.MoneyMap{}
	comp := costapi.MoneyMap{}
	tot := costapi.MoneyMap{}
	addMoney(llm, a.Cost)
	addMoney(comp, a.Compute.Cost)
	addMoney(tot, a.TotalCost)

	var descActors []costapi.ActorCost
	var descRaw []json.RawMessage
	if isCanonical {
		rootKey := taskKey(a.Atespace, a.Name)
		for _, descKey := range taskDescendants(children, rootKey) {
			ns, name, _ := strings.Cut(descKey, "/")
			cIdx := costapi.CanonicalActorIndex(actors, ns, name)
			if cIdx >= 0 {
				ca := actors[cIdx]
				addMoney(llm, ca.Cost)
				addMoney(comp, ca.Compute.Cost)
				addMoney(tot, ca.TotalCost)
				descActors = append(descActors, ca)
				if cIdx < len(rawActors) {
					descRaw = append(descRaw, rawActors[cIdx])
				} else if b, err := json.Marshal(ca); err == nil {
					descRaw = append(descRaw, b)
				}
			} else {
				zero := costapi.ActorCost{
					Atespace: ns,
					Name:     name,
					Class:    "agent",
					IsAgent:  true,
				}
				descActors = append(descActors, zero)
				if b, err := json.Marshal(zero); err == nil {
					descRaw = append(descRaw, b)
				}
			}
		}
	}
	a.SubAgents = len(descActors)
	a.TreeLLMCost = roundMoney(llm)
	a.TreeComputeCost = roundMoney(comp)
	a.TreeTotalCost = roundMoney(tot)
	return descActors, descRaw
}

func applyReportTaskRollups(report *costapi.CostReport, tasks map[string]*v1alpha1.Task) {
	if report == nil {
		return
	}
	children := buildTaskChildren(tasks)
	for i := range report.Actors {
		a := &report.Actors[i]
		if a.Note != "" {
			continue
		}
		cIdx := costapi.CanonicalActorIndex(report.Actors, a.Atespace, a.Name)
		isCanonical := i == cIdx || cIdx == -1
		rollupActorTree(a, isCanonical, report.Actors, nil, children)
	}
}

func resolveActorDetail(ctx context.Context, client *costapi.Client, axServerURL, atespace string, atespaceExplicit bool, target string) (*costapi.ActorMatch, *v1alpha1.Task, error) {
	m, err := client.GetActorCost(ctx, atespace, atespaceExplicit, target)
	if err == nil {
		tasks := lookupAXTasks(ctx, axServerURL, m.Actor.Atespace)
		task := tasks[taskKey(m.Actor.Atespace, m.Actor.Name)]
		if task == nil && len(tasks) == 0 {
			task = lookupAXTask(ctx, axServerURL, m.Actor.Atespace, m.Actor.Name)
			if task != nil {
				tasks = map[string]*v1alpha1.Task{taskKey(m.Actor.Atespace, m.Actor.Name): task}
			}
		}
		children := buildTaskChildren(tasks)
		m.Children, m.SubAgents = rollupActorTree(m.Actor, m.Canonical, m.Report.Actors, m.RawActors, children)
		return m, task, nil
	}

	var notFound *costapi.NotFoundError
	if !errors.As(err, &notFound) || axServerURL == "" {
		return nil, nil, err
	}

	// The Cost Service lacks a record for this actor; fall back to the task in ax-server
	// when it exists, with $0 own cost and any descendants' Cost Service costs.
	targetAtespace := atespace
	targetName := strings.TrimSpace(target)
	if parts := strings.SplitN(targetName, "/", 2); len(parts) == 2 {
		targetAtespace, targetName, atespaceExplicit = parts[0], parts[1], true
	}
	task := lookupAXTask(ctx, axServerURL, targetAtespace, targetName)
	var tasks map[string]*v1alpha1.Task
	if task == nil && !atespaceExplicit {
		allTasks := lookupAXTasks(ctx, axServerURL, "")
		var matches []*v1alpha1.Task
		for _, t := range allTasks {
			if t.GetMetadata().GetName() == targetName {
				matches = append(matches, t)
			}
		}
		if len(matches) == 1 {
			task = matches[0]
			targetAtespace = firstNonEmpty(task.GetMetadata().GetAtespace(), "default")
			tasks = allTasks
		}
	}
	if task == nil {
		return nil, nil, err
	}
	targetAtespace = firstNonEmpty(task.GetMetadata().GetAtespace(), targetAtespace, "default")
	targetName = firstNonEmpty(task.GetMetadata().GetName(), targetName)
	if tasks == nil {
		tasks = lookupAXTasks(ctx, axServerURL, targetAtespace)
	}
	if tasks == nil {
		tasks = map[string]*v1alpha1.Task{}
	}
	tasks[taskKey(targetAtespace, targetName)] = task

	report, raw, rErr := client.GetCosts(ctx, targetAtespace)
	if rErr != nil {
		return nil, nil, rErr
	}
	var rawReport struct {
		Actors []json.RawMessage `json:"actors"`
	}
	_ = json.Unmarshal(raw, &rawReport)

	actor := &costapi.ActorCost{
		Atespace: targetAtespace,
		Name:     targetName,
		Class:    "agent",
		IsAgent:  true,
	}
	rawActor, _ := json.Marshal(actor)
	m = &costapi.ActorMatch{
		Actor:     actor,
		Raw:       rawActor,
		RawActors: rawReport.Actors,
		Canonical: true,
		Report:    report,
	}
	children := buildTaskChildren(tasks)
	m.Children, m.SubAgents = rollupActorTree(m.Actor, true, report.Actors, rawReport.Actors, children)
	return m, task, nil
}

// taskView is what the CLI shows about an actor's task: the AX server's
// record when it is reachable, else what the Cost Service reports.
type taskView struct {
	name, parent, phase, outcome string
	// outcomeSource is the Cost Service's outcomeSource; "" reads as "sandbox".
	outcomeSource string
	budgetUSD     float64
	exitCode      *int32
	finishedAt    string
}

func viewTask(a *costapi.ActorCost, task *v1alpha1.Task) *taskView {
	switch {
	case task != nil:
		v := &taskView{
			name:      task.GetMetadata().GetName(),
			parent:    task.GetSpec().GetParent(),
			phase:     task.GetStatus().GetPhase(),
			outcome:   task.GetStatus().GetOutcome(),
			budgetUSD: task.GetSpec().GetBudget().GetMaxUsd(),
		}
		if v.outcome != "" {
			code := task.GetStatus().GetExitCode()
			v.exitCode = &code
			if f := task.GetStatus().GetFinishedAt(); f != nil {
				v.finishedAt = f.AsTime().UTC().Format(time.RFC3339)
			}
			// ax-server's outcome is the runner's exit code; the Cost Service may know
			// of outside evidence for the same outcome.
			if a != nil && a.Task != nil && a.Task.Outcome == v.outcome {
				v.outcomeSource = a.Task.OutcomeSource
			}
		}
		return v
	case a != nil && a.Task != nil:
		return &taskView{name: a.Task.Name, parent: a.Task.Parent, outcome: a.Task.Outcome, outcomeSource: a.Task.OutcomeSource, budgetUSD: a.Task.BudgetUSD, exitCode: a.Task.ExitCode}
	}
	return nil
}

// overBudget reports whether spend passes a set budget.
func overBudget(spend, budget float64) bool { return budget > 0 && spend > budget }

func renderCostList(out io.Writer, report *costapi.CostReport, scope string, tasks map[string]*v1alpha1.Task, wide bool) error {
	applyReportTaskRollups(report, tasks)
	rows := report.Actors
	serverFiltered := scope == "" || report.Atespace == scope
	if !serverFiltered {
		// An older server ignores ?atespace=; show only the rows in scope.
		rows = nil
		for _, a := range report.Actors {
			if a.Atespace == scope {
				rows = append(rows, a)
			}
		}
	}

	views := make([]*taskView, len(rows))
	hasTasks := false
	for i := range rows {
		views[i] = viewTask(&rows[i], tasks[rows[i].Atespace+"/"+rows[i].Name])
		if views[i] != nil {
			hasTasks = true
		}
	}

	cols := []string{"NAME", "ATESPACE"}
	if len(tasks) > 0 {
		cols = append(cols, "PHASE")
	}
	if hasTasks {
		cols = append(cols, "OUTCOME")
	}
	cols = append(cols, "CLASS", "TEMPLATE")
	if wide {
		cols = append(cols, "UID")
	}
	cols = append(cols, "CALLS", "TOKENS(IN/OUT)", "LLM($)", "HOSTING($)", "TOTAL($)")
	if wide {
		cols = append(cols, "MEASURED($)", "SUB-AGENTS")
	}
	cols = append(cols, "TREE-TOTAL($)")
	if hasTasks {
		cols = append(cols, "SPEND/BUDGET($)")
	}

	w := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, strings.Join(cols, "\t"))
	for i, a := range rows {
		name, atespace, class := a.Name, a.Atespace, actorClass(a)
		if a.Note != "" {
			name, atespace, class = "("+a.Note+")", "-", "-"
		}
		v := views[i]
		cells := []string{name, atespace}
		if len(tasks) > 0 {
			phase := "-"
			if t := tasks[a.Atespace+"/"+a.Name]; t != nil {
				phase = firstNonEmpty(t.GetStatus().GetPhase(), "Pending")
			}
			cells = append(cells, phase)
		}
		if hasTasks {
			outcome := "-"
			if v != nil && v.outcome != "" {
				outcome = v.outcome
			}
			cells = append(cells, outcome)
		}
		cells = append(cells, class, firstNonEmpty(a.Template, "-"))
		if wide {
			cells = append(cells, firstNonEmpty(a.UID, "-"))
		}
		cells = append(cells, fmt.Sprint(a.Calls), fmt.Sprintf("%d/%d", a.InputTokens, a.OutputTokens),
			formatUSD(a.Cost.USD()), formatUSD(a.HostingCost.USD()), formatUSD(a.TotalCost.USD()))
		if wide {
			cells = append(cells, formatUSD(a.MeasuredHostingCost.USD()), fmt.Sprint(a.SubAgents))
		}
		cells = append(cells, formatUSD(a.TreeTotalCost.USD()))
		if hasTasks {
			cell := "-"
			if v != nil && v.budgetUSD > 0 {
				cell = formatUSD(a.TotalCost.USD()) + "/" + formatUSD(v.budgetUSD)
				if overBudget(a.TotalCost.USD(), v.budgetUSD) {
					cell += " OVER"
				}
			}
			cells = append(cells, cell)
		}
		fmt.Fprintln(w, strings.Join(cells, "\t"))
	}
	if err := w.Flush(); err != nil {
		return err
	}

	scopeLabel := "all atespaces"
	if scope != "" {
		scopeLabel = "atespace " + scope
	}
	if serverFiltered {
		t := report.Totals
		fmt.Fprintf(out, "\nSummary (%s, %d rows): calls=%d tokens=%d/%d llm=%s compute=%s total=%s\n",
			scopeLabel, len(rows), t.Calls, t.InputTokens, t.OutputTokens,
			formatUSD(t.Cost.USD()), formatUSD(report.Compute.Cost.USD()),
			formatUSD(report.TotalCost.USD()))
		if inFlight := report.InFlightComputeCost.USD(); inFlight > 0 {
			fmt.Fprintf(out, "In-flight compute: %s from %d open leases, accrued up to the report and included in compute\n",
				formatUSD(inFlight), report.Compute.OpenLeases)
		}
	} else {
		fmt.Fprintf(out, "\nSummary (%s, %d rows): the server did not filter by atespace, so it sent no totals for this atespace\n", scopeLabel, len(rows))
	}
	if oc := report.OutcomeCosts; oc != nil {
		fmt.Fprintf(out, "By task outcome (%s): completed=%s (%d tasks) failed=%s (%d tasks) unfinished=%s (%d tasks) no-task=%s (own LLM + compute)\n",
			outcomeEvidence(oc.OutcomeSource),
			formatUSD(oc.Completed.TotalCost.USD()), oc.Completed.Tasks,
			formatUSD(oc.Failed.TotalCost.USD()), oc.Failed.Tasks,
			formatUSD(oc.Unfinished.TotalCost.USD()), oc.Unfinished.Tasks,
			formatUSD(oc.NoTask.TotalCost.USD()))
	}
	printCostBasis(out, report.Estimated())
	printCostWarnings(out, report.Totals, report.Compute, noteRows(rows))
	return nil
}

func actorClass(a costapi.ActorCost) string {
	if a.Class != "" {
		return a.Class
	}
	if a.IsAgent {
		return "agent"
	}
	return "shared"
}

func noteRows(rows []costapi.ActorCost) []costapi.ActorCost {
	var out []costapi.ActorCost
	for _, a := range rows {
		if a.Note != "" {
			out = append(out, a)
		}
	}
	return out
}

// estimateLabel marks numbers that come from rate card prices and that no bill
// has confirmed.
const estimateLabel = "ESTIMATE — NOT RECONCILED"

func printCostBasis(out io.Writer, estimated bool) {
	if estimated {
		fmt.Fprintf(out, "Cost basis: %s (rate card prices; no bill was reconciled)\n", estimateLabel)
	}
}

// printCostWarnings prints one warning line when some spend is not in the
// numbers: calls with unknown or no price, leases with no price, non-ok usage
// reports, or spend that the Cost Service could not put on an actor.
func printCostWarnings(out io.Writer, totals costapi.Totals, compute costapi.ComputeSummary, notes []costapi.ActorCost) {
	var parts []string
	if n := totals.UnknownCostCalls; n > 0 {
		parts = append(parts, fmt.Sprintf("%d calls with unknown cost (no usage report)", n))
	}
	if n := totals.UnpricedCalls; n > 0 {
		parts = append(parts, fmt.Sprintf("%d calls with no price", n))
	}
	if n := compute.UnpricedLeases; n > 0 {
		parts = append(parts, fmt.Sprintf("%d leases with no price", n))
	}
	var bad []string
	for status, n := range totals.UsageStatus {
		if status != "ok" && n > 0 {
			bad = append(bad, fmt.Sprintf("%s=%d", status, n))
		}
	}
	sort.Strings(bad)
	if len(bad) > 0 {
		parts = append(parts, "usage status "+strings.Join(bad, " "))
	}
	for _, a := range notes {
		parts = append(parts, fmt.Sprintf("%s: %d calls, %s", a.Note, a.Calls, formatUSD(a.TotalCost.USD())))
	}
	if len(parts) > 0 {
		fmt.Fprintf(out, "Warning: these numbers leave out spend: %s\n", strings.Join(parts, "; "))
	}
}

// actorDetailExport is the -o json/yaml form of `ax cost task`. The Cost Service
// rows are the server's JSON as it came; axTask is the AX server's task.
type actorDetailExport struct {
	Actor     json.RawMessage   `json:"actor"`
	Phase     string            `json:"phase,omitempty"`
	AXTask    json.RawMessage   `json:"axTask,omitempty"`
	CostBasis string            `json:"costBasis,omitempty"`
	Sessions  []json.RawMessage `json:"sessions,omitempty"`
	SubAgents []json.RawMessage `json:"subAgents,omitempty"`
}

func renderActorDetail(out io.Writer, m *costapi.ActorMatch, task *v1alpha1.Task, outputFormat string, budgets *costapi.BudgetsReport) error {
	a := m.Actor
	if isStructured(outputFormat) {
		exp := actorDetailExport{
			Actor:     m.Raw,
			Phase:     task.GetStatus().GetPhase(),
			CostBasis: m.Report.CostBasis,
			Sessions:  m.Sessions,
			SubAgents: m.SubAgents,
		}
		if task != nil {
			if b, err := protojson.Marshal(task); err == nil {
				exp.AXTask = b
			}
		}
		raw, err := json.MarshalIndent(exp, "", "  ")
		if err != nil {
			return err
		}
		return writeRaw(out, raw, outputFormat)
	}

	fmt.Fprintf(out, "Name:               %s\n", a.Name)
	fmt.Fprintf(out, "Atespace:           %s\n", a.Atespace)
	if phase := task.GetStatus().GetPhase(); phase != "" {
		fmt.Fprintf(out, "Task Phase:         %s\n", phase)
	}
	if a.UID != "" {
		fmt.Fprintf(out, "Actor UID:          %s (gen_ai.agent.id)\n", a.UID)
	}
	if a.Template != "" {
		fmt.Fprintf(out, "Template:           %s (ate.template.name)\n", a.Template)
	}
	fmt.Fprintf(out, "Class:              %s (is_agent=%t)\n", actorClass(*a), a.IsAgent)
	if a.Tenant != "" {
		fmt.Fprintf(out, "Tenant:             %s (agent_cost.tenant)\n", a.Tenant)
	}
	fmt.Fprintf(out, "Sub-Agents:         %d\n", a.SubAgents)
	printTaskBudget(out, a, viewTask(a, task))
	printBudgetState(out, budgets, a.Atespace, a.Name, task.GetStatus().GetBudgetGroup())
	printHolds(out, task.GetStatus().GetHolds())

	printActorCostSections(out, a, m.SessionCosts, m.Children)
	fmt.Fprintln(out)
	printCostBasis(out, m.Report.Estimated())
	printCostWarnings(out, costapi.Totals{UnknownCostCalls: a.UnknownCostCalls, UnpricedCalls: a.UnpricedCalls, UsageStatus: a.UsageStatus}, a.Compute, nil)
	return nil
}

// outcomeEvidence labels a task outcome by who decided it. The runner's exit
// code comes from inside the sandbox, so an outcome with no outside evidence
// (or from a server that predates outcomeSource) is self-reported.
func outcomeEvidence(source string) string {
	if source == "" || source == "sandbox" {
		return "self-reported"
	}
	return "evidence: " + source
}

// printTaskBudget prints the task's place in the tree, its outcome, and its
// spend (LLM plus compute) against its budget.
func printTaskBudget(out io.Writer, a *costapi.ActorCost, v *taskView) {
	if v == nil {
		return
	}
	if v.parent != "" {
		fmt.Fprintf(out, "Parent Task:        %s\n", v.parent)
	}
	switch {
	case v.outcome != "" && v.exitCode != nil:
		finished := ""
		if v.finishedAt != "" {
			finished = ", finished " + v.finishedAt
		}
		fmt.Fprintf(out, "Outcome:            %s (%s: exit code %d%s)\n", v.outcome, outcomeEvidence(v.outcomeSource), *v.exitCode, finished)
	case v.outcome != "":
		fmt.Fprintf(out, "Outcome:            %s (%s)\n", v.outcome, outcomeEvidence(v.outcomeSource))
	default:
		fmt.Fprintf(out, "Outcome:            - (the command has not exited)\n")
	}
	own, tree := a.TotalCost.USD(), a.TreeTotalCost.USD()
	if v.budgetUSD <= 0 {
		fmt.Fprintf(out, "Budget:             none\n")
		fmt.Fprintf(out, "Spend (LLM+compute): own %s, tree %s\n", formatUSD(own), formatUSD(tree))
		return
	}
	fmt.Fprintf(out, "Budget:             %s\n", formatUSD(v.budgetUSD))
	flag := ""
	switch {
	case overBudget(own, v.budgetUSD):
		flag = "  OVER BUDGET"
	case overBudget(tree, v.budgetUSD):
		flag = "  OVER BUDGET (with sub-agents)"
	}
	fmt.Fprintf(out, "Spend (LLM+compute): own %s (%.1f%% of budget), tree %s (%.1f%%)%s\n",
		formatUSD(own), 100*own/v.budgetUSD, formatUSD(tree), 100*tree/v.budgetUSD, flag)
}
func printActorCostSections(out io.Writer, a *costapi.ActorCost, sessions []costapi.SessionCost, children []costapi.ActorCost) {
	// Per-actor costs come from the Cost Service; tree totals are rolled up from the Ax
	// task tree.
	bookedCompute := a.Compute.Cost.USD()
	measuredCompute := a.MeasuredComputeCost.USD()
	storageUSD := a.StorageCost.USD()
	egressUSD := a.EgressCost.USD()

	hostingBooked := a.HostingCost.USD()
	if hostingBooked == 0 {
		hostingBooked = a.VariableHostingUSD()
	}
	hostingMeasured := a.MeasuredHostingCost.USD()
	if hostingMeasured == 0 {
		hostingMeasured = a.MeasuredVariableHostingUSD()
	}

	fmt.Fprintln(out, "\nHosting Cost Breakdown (Active Cost):")
	hw := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	fmt.Fprintln(hw, "  COMPONENT\tBOOKED-SHARE ($)\tMEASURED-USAGE ($)\tDETAILS")
	fmt.Fprintf(hw, "  1. Active Hosting Cost\t%s\t%s\tcompute + snapshot storage + egress\n",
		formatUSD(hostingBooked), formatUSD(hostingMeasured))
	fmt.Fprintf(hw, "     - Compute Leases\t%s\t%s\tleases=%d duration=%.1fs booked=%.4f vCPU-h / %.4f GiB-h measured=%.2f CPU-s\n",
		formatUSD(bookedCompute), formatUSD(measuredCompute),
		a.Compute.Leases, a.Compute.LeaseSeconds,
		a.Compute.VCPUSeconds/3600.0, a.Compute.GiBSeconds/3600.0,
		a.Compute.MeasuredCPUSeconds)
	if inFlight := a.Compute.InFlightComputeCost.USD(); inFlight > 0 {
		fmt.Fprintf(hw, "       of which in flight\t%s\t-\topen leases=%d, accrued up to the report\n",
			formatUSD(inFlight), a.Compute.OpenLeases)
	}
	fmt.Fprintf(hw, "     - Snapshot Storage\t%s\t%s\tGCS / snapshot GiB-month share\n",
		formatUSD(storageUSD), formatUSD(storageUSD))
	if egressUSD > 0 {
		fmt.Fprintf(hw, "     - Egress Network\t%s\t%s\tmetered internet egress\n",
			formatUSD(egressUSD), formatUSD(egressUSD))
	}
	_ = hw.Flush()

	fmt.Fprintln(out, "\nModel Token & Inference Cost:")
	mw := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	fmt.Fprintln(mw, "  PROVIDER\tMODEL\tCALLS\tINPUT\tCACHE-READ\tCACHE-WRITE\tOUTPUT\tREASONING\tCOST ($)")
	if len(a.Models) > 0 {
		for _, m := range a.Models {
			fmt.Fprintf(mw, "  %s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%s\n",
				m.Provider, m.Model, m.Calls, m.InputTokens,
				m.CacheReadInputTokens, m.CacheWriteInputTokens,
				m.OutputTokens, m.ReasoningOutputTokens, formatUSD(m.Cost.USD()))
		}
	}
	fmt.Fprintf(mw, "  TOTAL\t-\t%d\t%d\t%d\t%d\t%d\t%d\t%s\n",
		a.Calls, a.InputTokens, a.CacheReadInputTokens, a.CacheWriteInputTokens,
		a.OutputTokens, a.ReasoningOutputTokens, formatUSD(a.Cost.USD()))
	_ = mw.Flush()

	ownDirect := a.TotalCost.USD()
	treeLLM := a.TreeLLMCost.USD()
	treeCompute := a.TreeComputeCost.USD()
	treeDirect := a.TreeTotalCost.USD()

	fmt.Fprintln(out, "\nTotal Cost Summary (Own vs. Sub-Agent Tree):")
	tw := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	fmt.Fprintln(tw, "  SCOPE\tLLM ($)\tCOMPUTE ($)\tLLM + COMPUTE ($)\tHOSTING (BOOKED $)\tHOSTING (MEASURED $)")
	fmt.Fprintf(tw, "  Own (%s/%s)\t%s\t%s\t%s\t%s\t%s\n",
		a.Atespace, a.Name, formatUSD(a.Cost.USD()), formatUSD(bookedCompute), formatUSD(ownDirect), formatUSD(hostingBooked), formatUSD(hostingMeasured))
	if a.SubAgents > 0 || len(children) > 0 || treeDirect > ownDirect {
		fmt.Fprintf(tw, "  Tree (Own + %d Sub-Agents)\t%s\t%s\t%s\t-\t-\n",
			a.SubAgents, formatUSD(treeLLM), formatUSD(treeCompute), formatUSD(treeDirect))
	}
	_ = tw.Flush()

	if len(children) > 0 {
		fmt.Fprintln(out, "\nSub-Agents:")
		cw := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
		fmt.Fprintln(cw, "  NAME\tATESPACE\tUID\tTEMPLATE\tCALLS\tLLM ($)\tHOSTING ($)\tTOTAL ($)")
		for _, c := range children {
			fmt.Fprintf(cw, "  %s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\n",
				c.Name, c.Atespace, firstNonEmpty(c.UID, "-"), firstNonEmpty(c.Template, "-"), c.Calls,
				formatUSD(c.Cost.USD()), formatUSD(c.HostingCost.USD()), formatUSD(c.TotalCost.USD()))
		}
		_ = cw.Flush()
	}

	if len(sessions) > 0 {
		fmt.Fprintln(out, "\nSessions (gen_ai.conversation.id / ate.actor.epoch):")
		sw := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
		fmt.Fprintln(sw, "  SESSION-ID\tSOURCE\tEPOCH\tCALLS\tTOKENS(IN/OUT)\tACTIVE(S)\tSHARE\tLLM ($)\tCOMPUTE ($)\tTOTAL ($)")
		for _, s := range sessions {
			src := "epoch"
			if s.FromHeader {
				src = "header"
			}
			epoch := s.Epoch
			if epoch == "" {
				epoch = "-"
			}
			fmt.Fprintf(sw, "  %s\t%s\t%s\t%d\t%d/%d\t%.1f\t%.1f%%\t%s\t%s\t%s\n",
				s.SessionID, src, epoch, s.Calls, s.InputTokens, s.OutputTokens,
				s.ActiveSeconds, s.ActiveShare*100.0,
				formatUSD(s.LLMCost.USD()), formatUSD(s.ComputeCost.USD()), formatUSD(s.TotalCost.USD()))
		}
		_ = sw.Flush()
	}
}

func renderCurveReport(out io.Writer, curve *costapi.CurveReport) error {
	fmt.Fprintf(out, "Hosting Cost Sensitivity Curve (< $%.2f / agent-month claim)\n", curve.TargetUSD)
	fmt.Fprintf(out, "  Storage / Agent-Mo:    %s\n\n", formatUSD(curve.StoragePerAgentUSD))

	tw := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	fmt.Fprintln(tw, "TIER\tVARIABLE ($/ACT-HR)\tFIXED ($/MO)\tMAX HOURS (< TARGET)\tALWAYS-ON (730H)")
	for _, t := range curve.Tiers {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%.2f h/mo (%.2f h/day)\t$%.2f/mo\n",
			t.Tier,
			formatUSD(t.PerActiveHour),
			formatUSD(t.Fixed),
			t.BreakEvenHours,
			t.BreakEvenHours/30.4375,
			t.AlwaysOn,
		)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	if len(curve.Tiers) > 0 && len(curve.Tiers[0].Points) > 0 {
		fmt.Fprintln(out, "\nActive Hosting Cost ($ / agent-month) by Active Hours:")
		pw := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
		var header strings.Builder
		header.WriteString("  ACTIVE-HRS/MO")
		for _, t := range curve.Tiers {
			header.WriteString("\t" + strings.ToUpper(t.Tier) + " ($)")
		}
		header.WriteString("\tUNDER-TARGET TIERS")
		fmt.Fprintln(pw, header.String())

		numPoints := len(curve.Tiers[0].Points)
		for idx := 0; idx < numPoints; idx++ {
			hrs := curve.Tiers[0].Points[idx].ActiveHours
			var row strings.Builder
			row.WriteString(fmt.Sprintf("  %.1f h/mo", hrs))
			var passing []string
			for _, t := range curve.Tiers {
				if idx < len(t.Points) {
					pt := t.Points[idx]
					row.WriteString(fmt.Sprintf("\t$%.4f", pt.ActiveCost))
					if pt.UnderTarget {
						passing = append(passing, t.Tier)
					}
				} else {
					row.WriteString("\t-")
				}
			}
			passStr := "none"
			if len(passing) > 0 {
				passStr = strings.Join(passing, ", ")
			}
			row.WriteString("\t" + passStr)
			fmt.Fprintln(pw, row.String())
		}
		if err := pw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// renderFocusReport prints the FOCUS 1.3 allocation keys. They hold usage
// only: the holder of the bill divides each key by its resource's total and
// writes the split cost rows, so there is no cost column here.
func renderFocusReport(out io.Writer, focus *costapi.FocusReport) error {
	fmt.Fprintf(out, "FOCUS 1.3 Allocation Keys\n")
	if focus.ChargePeriodStart != "" || focus.ChargePeriodEnd != "" {
		fmt.Fprintf(out, "  Charge Period:   %s to %s\n", firstNonEmpty(focus.ChargePeriodStart, "-"), firstNonEmpty(focus.ChargePeriodEnd, "-"))
	}
	fmt.Fprintf(out, "  Basis:           usage keys, no cost (the bill holder writes the split cost rows)\n\n")
	if len(focus.AllocationKeys) == 0 {
		fmt.Fprintln(out, "No allocation keys.")
		return nil
	}
	w := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "ALLOCATED-RESOURCE-ID\tALLOCATED-NAME\tMETHOD\tRESOURCE\tUSAGE\tTENANT\tCLASS")
	for _, k := range focus.AllocationKeys {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			firstNonEmpty(k.AllocatedResourceID, "-"), firstNonEmpty(k.AllocatedResourceName, "-"),
			k.AllocatedMethodID, firstNonEmpty(k.ResourceID, k.ServiceName, "-"),
			focusUsage(k), focusTenant(k), focusClass(k))
	}
	return w.Flush()
}

// focusUsage joins a key's elements as "<quantity> <unit>" pairs.
func focusUsage(k costapi.AllocationKey) string {
	var parts []string
	for _, e := range k.AllocatedMethodDetails.Elements {
		unit := e.UsageUnit
		if e.Currency != "" {
			unit += " (" + e.Currency + ")"
		}
		parts = append(parts, fmt.Sprintf("%.4f %s", e.UsageQuantity, unit))
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, ", ")
}

func focusTenant(k costapi.AllocationKey) string {
	return firstNonEmpty(k.Tag("tenant"), "-")
}

func focusClass(k costapi.AllocationKey) string {
	if class := k.Tag("class"); class != "" {
		return class
	}
	switch k.Tag("is_agent") {
	case "true":
		return "agent"
	case "false":
		return "shared"
	}
	return "-"
}

// writeRaw prints a server JSON body as JSON, unchanged, or as YAML with the
// same keys in the same order. It never goes through the client's structs,
// so no field that the CLI does not know is lost.
func writeRaw(out io.Writer, raw []byte, format string) error {
	switch format {
	case "json":
		raw = bytes.TrimRight(raw, "\n")
		if _, err := out.Write(raw); err != nil {
			return err
		}
		_, err := io.WriteString(out, "\n")
		return err
	case "yaml":
		y, err := jsonToYAML(raw)
		if err != nil {
			return err
		}
		_, err = out.Write(y)
		return err
	default:
		return fmt.Errorf("unsupported structured format %q", format)
	}
}

// jsonToYAML converts a JSON document to block-style YAML, keeping key order.
// A string that would read back as another type, such as "true" or a date,
// stays quoted.
func jsonToYAML(raw []byte) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("converting the server's JSON to YAML: %w", err)
	}
	var plain func(n *yaml.Node)
	plain = func(n *yaml.Node) {
		n.Style = 0
		for _, c := range n.Content {
			plain(c)
		}
	}
	plain(&doc)
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func formatUSD(v float64) string {
	if v < 0 {
		return fmt.Sprintf("-$%.6f", -v)
	}
	return fmt.Sprintf("$%.6f", v)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// resolveOptionalAXServerURL returns an AX gRPC server URL if one is explicitly
// configured or already has a healthy local tunnel, without spawning a new
// kubectl port-forward when ax-system is not installed.
func resolveOptionalAXServerURL(explicitServer, kubeContext string) string {
	if explicitServer != "" {
		return explicitServer
	}
	if env := os.Getenv("AX_SERVER"); env != "" {
		return env
	}
	ctxName, err := tunnel.CurrentContext(kubeContext)
	if err != nil || ctxName == "" {
		return ""
	}
	if existing, err := tunnel.GetTunnel(ctxName); err == nil && existing != nil && tunnel.IsTunnelHealthy(existing.Port) {
		return fmt.Sprintf("http://127.0.0.1:%d", existing.Port)
	}
	return ""
}

// resolveOptionalCostURL returns a Cost Service HTTP API URL if one is explicitly
// configured or reachable through the tunnel saved for the kube context. It
// never uses a local port that no saved tunnel names for the context.
func resolveOptionalCostURL(costServer, kubeContext string) string {
	if costServer != "" {
		return costServer
	}
	if env := tunnel.CostServerFromEnv(); env != "" {
		return env
	}
	if ctxName, err := tunnel.CurrentContext(kubeContext); err == nil && ctxName != "" {
		if existing, err := tunnel.GetTunnel(tunnel.CostTunnelKey(ctxName)); err == nil && existing != nil && tunnel.IsCostTunnelHealthy(existing.Port) {
			return fmt.Sprintf("http://127.0.0.1:%d", existing.Port)
		}
	}
	return ""
}

// tryPrintTaskDescribeCost appends Cost Service cost attribution to `ax describe task`
// when a Cost Service is reachable and has records for the task's actor or
// its sub-agent tasks.
func tryPrintTaskDescribeCost(out io.Writer, costURL string, task *v1alpha1.Task, allTasks ...map[string]*v1alpha1.Task) {
	if costURL == "" || task.GetMetadata() == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client := costapi.NewClient(costURL, nil)
	atespace := firstNonEmpty(task.GetMetadata().GetAtespace(), "default")
	name := task.GetMetadata().GetName()
	var taskMap map[string]*v1alpha1.Task
	if len(allTasks) > 0 {
		taskMap = allTasks[0]
	}
	if taskMap != nil {
		taskMap[taskKey(atespace, name)] = task
	}
	children := buildTaskChildren(taskMap)
	budgets := fetchBudgets(ctx, client, atespace)
	group := task.GetStatus().GetBudgetGroup()
	// printBudgetOnly shows the budget state of a task with no cost records.
	printBudgetOnly := func() {
		if len(budgets.BudgetsFor(atespace, name, group)) > 0 {
			fmt.Fprintln(out, "\nCost:")
			printBudgetState(out, budgets, atespace, name, group)
		}
	}

	// The actor is named after the task, in the task's atespace.
	m, err := client.GetActorCost(ctx, atespace, true, name)
	var amb *costapi.AmbiguousError
	if errors.As(err, &amb) {
		fmt.Fprintf(out, "\nCost Service: %v\n", err)
		return
	}
	if err != nil {
		var notFound *costapi.NotFoundError
		if !errors.As(err, &notFound) || len(children) == 0 {
			printBudgetOnly()
			return
		}
		report, raw, rErr := client.GetCosts(ctx, atespace)
		if rErr != nil {
			return
		}
		hasDescendantCost := false
		for _, descKey := range taskDescendants(children, taskKey(atespace, name)) {
			ns, dName, _ := strings.Cut(descKey, "/")
			if costapi.CanonicalActorIndex(report.Actors, ns, dName) >= 0 {
				hasDescendantCost = true
				break
			}
		}
		if !hasDescendantCost {
			printBudgetOnly()
			return
		}
		var rawReport struct {
			Actors []json.RawMessage `json:"actors"`
		}
		_ = json.Unmarshal(raw, &rawReport)
		actor := &costapi.ActorCost{
			Atespace: atespace,
			Name:     name,
			Class:    "agent",
			IsAgent:  true,
		}
		rawActor, _ := json.Marshal(actor)
		m = &costapi.ActorMatch{
			Actor:     actor,
			Raw:       rawActor,
			RawActors: rawReport.Actors,
			Canonical: true,
			Report:    report,
		}
	}
	m.Children, m.SubAgents = rollupActorTree(m.Actor, m.Canonical, m.Report.Actors, m.RawActors, children)
	fmt.Fprintln(out, "\nCost:")
	printTaskBudget(out, m.Actor, viewTask(m.Actor, task))
	printBudgetState(out, budgets, atespace, name, group)
	printActorCostSections(out, m.Actor, m.SessionCosts, m.Children)
	fmt.Fprintln(out)
	printCostBasis(out, m.Report.Estimated())
	printCostWarnings(out, costapi.Totals{UnknownCostCalls: m.Actor.UnknownCostCalls, UnpricedCalls: m.Actor.UnpricedCalls, UsageStatus: m.Actor.UsageStatus}, m.Actor.Compute, nil)
}
