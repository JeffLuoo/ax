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
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/google/ax/internal/costapi"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

const budgetUsage = `Usage:
  ax budget set [task] <name> --max-usd <usd> [--warn-at <fraction>] [--scope Tree|Task]
  ax budget clear [task] <name>

Set or remove a task's budget (spec.budget). Ax labels the task's actor, and
every actor under it for a Tree budget, with agent-cost/budget-* labels; the
Cost Service tracks the spend and holds the actors when the budget runs out.
Raising the limit of a held tree lets its tasks resume with their state.

  --max-usd   Limit in US dollars on model tokens and compute together.
  --warn-at   Fraction of the limit at which the budget turns WARN, for
              example 0.8. Default: the Cost Service default.
  --scope     Tree (default) covers the task and its sub-tasks; Task covers
              only this task.

Examples:
  ax budget set planner --max-usd 5 --warn-at 0.8
  ax budget clear planner`

type budgetOptions struct {
	action string // "set" or "clear"
	name   string
	budget *v1alpha1.Budget
	help   bool
}

func parseBudgetArgs(args []string) (budgetOptions, error) {
	var opts budgetOptions
	var positional []string
	b := &v1alpha1.Budget{}
	maxSet := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		name, value, hasValue := strings.Cut(arg, "=")
		takeValue := func() (string, error) {
			if hasValue {
				return value, nil
			}
			if i+1 >= len(args) {
				return "", fmt.Errorf("%s requires a value", name)
			}
			i++
			return args[i], nil
		}
		switch name {
		case "-h", "--help", "help":
			opts.help = true
			return opts, nil
		case "--max-usd":
			v, err := takeValue()
			if err != nil {
				return opts, err
			}
			f, err := strconv.ParseFloat(strings.TrimPrefix(v, "$"), 64)
			if err != nil {
				return opts, fmt.Errorf("--max-usd: %q is not a number", v)
			}
			b.MaxUsd = f
			maxSet = true
		case "--warn-at":
			v, err := takeValue()
			if err != nil {
				return opts, err
			}
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return opts, fmt.Errorf("--warn-at: %q is not a number", v)
			}
			b.WarnAt = f
		case "--scope":
			v, err := takeValue()
			if err != nil {
				return opts, err
			}
			switch strings.ToLower(v) {
			case "tree":
				b.Scope = v1alpha1.BudgetScopeTree
			case "task":
				b.Scope = v1alpha1.BudgetScopeTask
			default:
				return opts, fmt.Errorf("--scope: %q is not Tree or Task", v)
			}
		default:
			if strings.HasPrefix(arg, "-") {
				return opts, fmt.Errorf("unknown flag %q", arg)
			}
			positional = append(positional, arg)
		}
	}
	if len(positional) > 0 && (positional[0] == "set" || positional[0] == "clear") {
		opts.action = positional[0]
		positional = positional[1:]
	}
	if len(positional) > 0 && (positional[0] == "task" || positional[0] == "tasks") {
		positional = positional[1:]
	}
	if opts.action == "" || len(positional) != 1 {
		return opts, fmt.Errorf("usage: ax budget set|clear [task] <name>")
	}
	opts.name = positional[0]
	switch opts.action {
	case "set":
		if !maxSet || b.MaxUsd <= 0 {
			return opts, fmt.Errorf("ax budget set needs --max-usd greater than 0; use ax budget clear to remove a limit")
		}
		if err := v1alpha1.ValidateBudget(b); err != nil {
			return opts, err
		}
		opts.budget = b
	case "clear":
		if maxSet || b.WarnAt != 0 || b.Scope != "" {
			return opts, fmt.Errorf("ax budget clear takes no flags")
		}
	}
	return opts, nil
}

func runBudget(serverURL, atespace string, args []string) error {
	opts, err := parseBudgetArgs(args)
	if opts.help {
		fmt.Println(budgetUsage)
		return nil
	}
	if err != nil {
		return err
	}
	client, conn, err := getAXClient(serverURL)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	task, err := client.UpdateTaskBudget(ctx, &v1alpha1.UpdateTaskBudgetRequest{Atespace: atespace, Name: opts.name, Budget: opts.budget})
	if err != nil {
		return fmt.Errorf("updating the budget of task %q: %w", opts.name, err)
	}
	if opts.budget == nil {
		fmt.Printf("task/%s budget cleared\n", opts.name)
	} else {
		fmt.Printf("task/%s budget set to %s\n", opts.name, describeBudget(task.GetSpec().GetBudget()))
	}
	if g := task.GetStatus().GetBudgetGroup(); g != "" {
		fmt.Printf("budget group: %s\n", g)
	}
	printHolds(os.Stdout, task.GetStatus().GetHolds())
	return nil
}

// describeBudget renders a task budget as "$5 (warn at 80%, scope Tree)".
func describeBudget(b *v1alpha1.Budget) string {
	if b.GetMaxUsd() <= 0 {
		return "none"
	}
	scope := b.GetScope()
	if scope == "" {
		scope = v1alpha1.BudgetScopeTree
	}
	var parts []string
	if b.GetWarnAt() > 0 {
		parts = append(parts, fmt.Sprintf("warn at %s%%", strconv.FormatFloat(100*b.GetWarnAt(), 'f', -1, 64)))
	}
	parts = append(parts, "scope "+scope)
	return fmt.Sprintf("$%s (%s)", strconv.FormatFloat(b.GetMaxUsd(), 'f', -1, 64), strings.Join(parts, ", "))
}

// printHolds lists the holds on a task's actor. Any hold keeps the actor
// suspended until its owner removes it.
func printHolds(out io.Writer, holds []*v1alpha1.ActorHold) {
	if len(holds) == 0 {
		return
	}
	fmt.Fprintln(out, "Holds (the actor stays suspended until every hold is removed):")
	w := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "  NAME\tREASON\tCREATED BY\tSINCE")
	for _, h := range holds {
		since := ""
		if h.GetCreatedAt() != nil {
			since = h.GetCreatedAt().AsTime().UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(w, "  %s\t%s\t%s\t%s\n", h.GetName(), dashIfEmpty(h.GetReason()), dashIfEmpty(h.GetCreatedBy()), dashIfEmpty(since))
	}
	_ = w.Flush()
}

// holdNames joins the hold names for a table cell.
func holdNames(holds []*v1alpha1.ActorHold) string {
	if len(holds) == 0 {
		return "<none>"
	}
	names := make([]string, 0, len(holds))
	for _, h := range holds {
		names = append(names, h.GetName())
	}
	return strings.Join(names, ",")
}

func dashIfEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// budgetUsedPercent is spend as a percentage of the limit, or 0 with no limit.
func budgetUsedPercent(b costapi.BudgetState) float64 {
	if b.LimitUSD <= 0 {
		return 0
	}
	return 100 * b.TotalSpendUSD / b.LimitUSD
}

// renderBudgets prints `ax cost budgets`.
func renderBudgets(out io.Writer, r *costapi.BudgetsReport) error {
	budgets := append([]costapi.BudgetState(nil), r.Budgets...)
	sort.SliceStable(budgets, func(i, j int) bool { return budgets[i].ID < budgets[j].ID })
	mode := r.Mode
	if mode == "" {
		mode = "unknown"
	}
	fmt.Fprintf(out, "Mode: %s\n", mode)
	if mode == "observe-only" {
		fmt.Fprintln(out, "  (the Cost Service reports budget states but returns no 429 and adds no holds)")
	}
	if len(budgets) == 0 {
		fmt.Fprintln(out, "No budgets.")
	} else {
		fmt.Fprintln(out)
		w := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
		fmt.Fprintln(w, "BUDGET\tSTATE\tSPEND\tLIMIT\tUSED\tIN FLIGHT\tPERIOD\tHELD")
		for _, b := range budgets {
			period := b.Period
			if b.PeriodStart != "" && b.Period != "tree" {
				period += " from " + b.PeriodStart
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%.1f%%\t%s\t%s\t%d\n",
				b.ID, b.State, formatUSD(b.TotalSpendUSD), formatUSD(b.LimitUSD), budgetUsedPercent(b),
				formatUSD(b.InFlightComputeUSD), dashIfEmpty(period), len(b.HeldActors))
		}
		if err := w.Flush(); err != nil {
			return err
		}
	}
	if len(r.Holds) > 0 {
		fmt.Fprintln(out, "\nHolds:")
		w := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
		fmt.Fprintln(w, "  ACTOR\tHOLD\tREASON\tSINCE")
		for _, h := range r.Holds {
			fmt.Fprintf(w, "  %s\t%s\t%s\t%s\n", h.Actor, h.Name, dashIfEmpty(h.Reason), dashIfEmpty(h.CreatedAt))
		}
		if err := w.Flush(); err != nil {
			return err
		}
	}
	fmt.Fprintln(out)
	printCostBasis(out, true)
	return nil
}

// printBudgetState prints the live state of the budgets that cover a task's
// actor, from GET /v1/budgets. It prints nothing when no budget covers it.
func printBudgetState(out io.Writer, r *costapi.BudgetsReport, atespace, name, group string) {
	budgets := r.BudgetsFor(atespace, name, group)
	if len(budgets) == 0 {
		return
	}
	suffix := ""
	if r.Mode == "observe-only" {
		suffix = "  [observe-only: no holds]"
	}
	for i, b := range budgets {
		label := "Budget State:"
		if i > 0 {
			label = ""
		}
		fmt.Fprintf(out, "%-20s%s  %s of %s (%.1f%%)  %s%s\n", label, b.State,
			formatUSD(b.TotalSpendUSD), formatUSD(b.LimitUSD), budgetUsedPercent(b), b.ID, suffix)
	}
	for _, h := range r.HoldsFor(atespace, name) {
		reason := ""
		if h.Reason != "" {
			reason = ": " + h.Reason
		}
		fmt.Fprintf(out, "%-20s%s%s\n", "Cost Service Hold:", h.Name, reason)
	}
}

// fetchBudgets reads GET /v1/budgets, best effort: a Cost Service without
// budgets (or an older one) gives nil.
func fetchBudgets(ctx context.Context, client *costapi.Client, atespace string) *costapi.BudgetsReport {
	r, _, err := client.GetBudgets(ctx, atespace)
	if err != nil {
		return nil
	}
	return r
}
