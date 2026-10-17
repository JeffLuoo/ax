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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/google/ax/internal/finops"
	"github.com/google/ax/internal/tunnel"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"gopkg.in/yaml.v3"
)

const costUsage = `Usage:
  ax cost [tasks|actors] [-a <atespace> | -A] [-o table|wide|json|yaml]
  ax cost task <name> [-a <atespace>] [-o table|json|yaml]
  ax cost actor <name> [-a <atespace>] [-o table|json|yaml]
  ax cost curve [-o table|json|yaml]
  ax cost focus [-o table|json|csv|yaml]

Query Agent FinOps cost attribution, 3-number hosting cost breakdown
(booked-share vs. measured-usage), per-session spend, the < $1/agent-month
hosting sensitivity curve, and FOCUS 1.3 split cost allocation rows.

Examples:
  ax cost -a otel-bench
  ax cost -A -o wide
  ax cost task coding-parent-1 -a otel-bench
  ax cost curve
  ax cost focus
  ax cost focus -o csv`

type costOptions struct {
	subcmd       string // "list", "get", "curve", "focus"
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

// runCost executes `ax cost ...` against the FinOps HTTP server and optionally
// enriches task phases from the AX server when axServerURL is non-empty.
func runCost(finopsURL, axServerURL, atespace string, atespaceExplicit bool, args []string) error {
	opts, err := parseCostArgs(args)
	if err != nil {
		return err
	}
	if opts.help {
		fmt.Println(costUsage)
		return nil
	}

	client := finops.NewClient(finopsURL, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	switch opts.subcmd {
	case "curve":
		curve, err := client.GetCurve(ctx)
		if err != nil {
			return err
		}
		return renderCurveReport(os.Stdout, curve, opts.output)

	case "focus":
		if opts.output == "csv" {
			csvData, err := client.GetFocusCSV(ctx)
			if err != nil {
				return err
			}
			_, err = io.WriteString(os.Stdout, csvData)
			return err
		}
		focus, err := client.GetFocus(ctx)
		if err != nil {
			return err
		}
		return renderFocusReport(os.Stdout, focus, opts.output)

	case "get":
		scopeAtespace := atespace
		if opts.allAtespaces || !atespaceExplicit {
			// If the user did not explicitly pass -a, prefer matching in the default
			// atespace first, then fall back across all atespaces in GetActorCost.
			scopeAtespace = atespace
		}
		actor, sessions, children, _, err := client.GetActorCost(ctx, scopeAtespace, opts.target)
		if err != nil {
			return err
		}
		phase := lookupAXTaskPhase(ctx, axServerURL, actor.Atespace, actor.Name)
		return renderActorDetail(os.Stdout, actor, phase, sessions, children, opts.output)

	default:
		report, err := client.GetCosts(ctx)
		if err != nil {
			return err
		}
		filterAtespace := ""
		if !opts.allAtespaces && atespaceExplicit {
			filterAtespace = atespace
		} else if !opts.allAtespaces && hasActorsInAtespace(report.Actors, atespace) {
			filterAtespace = atespace
		}
		phases := lookupAXTaskPhases(ctx, axServerURL, filterAtespace)
		return renderCostList(os.Stdout, report, filterAtespace, phases, opts.output)
	}
}

func hasActorsInAtespace(actors []finops.ActorCost, atespace string) bool {
	if atespace == "" {
		return false
	}
	for _, a := range actors {
		if a.Atespace == atespace {
			return true
		}
	}
	return false
}

// lookupAXTaskPhases opportunistically queries the AX gRPC server for task
// phases if axServerURL is reachable. Errors are ignored so `ax cost` works on
// clusters running Substrate + FinOps without ax-server.
func lookupAXTaskPhases(ctx context.Context, axServerURL, atespace string) map[string]string {
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
	resp, err := client.ListTasks(qCtx, &v1alpha1.ListTasksRequest{Atespace: atespace})
	if err != nil {
		return nil
	}
	phases := make(map[string]string, len(resp.Tasks))
	for _, t := range resp.Tasks {
		if t.Metadata == nil {
			continue
		}
		phase := "Pending"
		if t.Status != nil && t.Status.Phase != "" {
			phase = t.Status.Phase
		}
		phases[t.Metadata.Atespace+"/"+t.Metadata.Name] = phase
		if t.Status != nil && t.Status.Actor != "" {
			phases[t.Metadata.Atespace+"/"+t.Status.Actor] = phase
		}
	}
	return phases
}

func lookupAXTaskPhase(ctx context.Context, axServerURL, atespace, name string) string {
	if axServerURL == "" {
		return ""
	}
	qCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	client, conn, err := getAXClient(axServerURL)
	if err != nil {
		return ""
	}
	defer conn.Close()
	t, err := client.GetTask(qCtx, &v1alpha1.GetTaskRequest{Atespace: atespace, Name: name})
	if err != nil || t == nil || t.Status == nil {
		return ""
	}
	return t.Status.Phase
}

func renderCostList(out io.Writer, report *finops.CostReport, filterAtespace string, phases map[string]string, outputFormat string) error {
	filtered := make([]finops.ActorCost, 0, len(report.Actors))
	for _, a := range report.Actors {
		if filterAtespace != "" && a.Atespace != filterAtespace {
			continue
		}
		filtered = append(filtered, a)
	}

	if outputFormat == "json" || outputFormat == "yaml" {
		outReport := *report
		outReport.Actors = filtered
		return encodeStructured(out, outReport, outputFormat)
	}

	w := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	hasPhases := len(phases) > 0
	wide := outputFormat == "wide"

	switch {
	case wide && hasPhases:
		fmt.Fprintln(w, "NAME\tATESPACE\tPHASE\tCLASS\tTEMPLATE\tUID\tCALLS\tTOKENS(IN/OUT)\tLLM($)\tHOSTING($)\tOWN-LOADED($)\tMEASURED($)\tSUB-AGENTS\tTREE-LOADED($)")
	case wide:
		fmt.Fprintln(w, "NAME\tATESPACE\tCLASS\tTEMPLATE\tUID\tCALLS\tTOKENS(IN/OUT)\tLLM($)\tHOSTING($)\tOWN-LOADED($)\tMEASURED($)\tSUB-AGENTS\tTREE-LOADED($)")
	case hasPhases:
		fmt.Fprintln(w, "NAME\tATESPACE\tPHASE\tCLASS\tTEMPLATE\tCALLS\tTOKENS(IN/OUT)\tLLM($)\tHOSTING($)\tOWN-LOADED($)\tTREE-LOADED($)")
	default:
		fmt.Fprintln(w, "NAME\tATESPACE\tCLASS\tTEMPLATE\tCALLS\tTOKENS(IN/OUT)\tLLM($)\tHOSTING($)\tOWN-LOADED($)\tTREE-LOADED($)")
	}

	var (
		sumCalls   int64
		sumIn      int64
		sumOut     int64
		sumLLM     float64
		sumHosting float64
		sumLoaded  float64
	)

	for _, a := range filtered {
		class := a.Class
		if class == "" {
			if a.IsAgent {
				class = "agent"
			} else {
				class = "shared"
			}
		}
		tmpl := a.Template
		if tmpl == "" {
			tmpl = "-"
		}
		uid := a.UID
		if uid == "" {
			uid = "-"
		}
		phase := phases[a.Atespace+"/"+a.Name]
		if phase == "" {
			phase = "-"
		}
		tokens := fmt.Sprintf("%d/%d", a.InputTokens, a.OutputTokens)
		llmUSD := a.Cost.USD()
		hostingUSD := a.HostingCost.USD()
		if hostingUSD == 0 {
			hostingUSD = a.VariableHostingUSD() + a.SharedOverheadUSD()
		}
		ownLoadedUSD := a.FullyLoadedCost.USD()
		if ownLoadedUSD == 0 {
			ownLoadedUSD = llmUSD + hostingUSD
		}
		measuredLoadedUSD := a.MeasuredFullyLoadedCost.USD()
		if measuredLoadedUSD == 0 {
			measuredLoadedUSD = llmUSD + a.MeasuredHostingCost.USD()
		}
		treeLoadedUSD := a.TreeFullyLoadedCost.USD()
		if treeLoadedUSD == 0 {
			treeLoadedUSD = ownLoadedUSD
		}

		sumCalls += a.Calls
		sumIn += a.InputTokens
		sumOut += a.OutputTokens
		sumLLM += llmUSD
		sumHosting += hostingUSD
		sumLoaded += ownLoadedUSD

		switch {
		case wide && hasPhases:
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n",
				a.Name, a.Atespace, phase, class, tmpl, uid, a.Calls, tokens,
				formatUSD(llmUSD), formatUSD(hostingUSD), formatUSD(ownLoadedUSD),
				formatUSD(measuredLoadedUSD), a.SubAgents, formatUSD(treeLoadedUSD))
		case wide:
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n",
				a.Name, a.Atespace, class, tmpl, uid, a.Calls, tokens,
				formatUSD(llmUSD), formatUSD(hostingUSD), formatUSD(ownLoadedUSD),
				formatUSD(measuredLoadedUSD), a.SubAgents, formatUSD(treeLoadedUSD))
		case hasPhases:
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
				a.Name, a.Atespace, phase, class, tmpl, a.Calls, tokens,
				formatUSD(llmUSD), formatUSD(hostingUSD), formatUSD(ownLoadedUSD),
				formatUSD(treeLoadedUSD))
		default:
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
				a.Name, a.Atespace, class, tmpl, a.Calls, tokens,
				formatUSD(llmUSD), formatUSD(hostingUSD), formatUSD(ownLoadedUSD),
				formatUSD(treeLoadedUSD))
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}

	scopeLabel := "all atespaces"
	if filterAtespace != "" {
		scopeLabel = "atespace " + filterAtespace
	}
	fmt.Fprintf(out, "\nSummary (%s, %d actors): calls=%d tokens=%d/%d llm=%s hosting=%s fully_loaded=%s\n",
		scopeLabel, len(filtered), sumCalls, sumIn, sumOut,
		formatUSD(sumLLM), formatUSD(sumHosting), formatUSD(sumLoaded))
	return nil
}

type actorDetailExport struct {
	Actor     *finops.ActorCost    `json:"actor" yaml:"actor"`
	Phase     string               `json:"phase,omitempty" yaml:"phase,omitempty"`
	Sessions  []finops.SessionCost `json:"sessions,omitempty" yaml:"sessions,omitempty"`
	SubAgents []finops.ActorCost   `json:"subAgents,omitempty" yaml:"subAgents,omitempty"`
}

func renderActorDetail(out io.Writer, a *finops.ActorCost, phase string, sessions []finops.SessionCost, children []finops.ActorCost, outputFormat string) error {
	if outputFormat == "json" || outputFormat == "yaml" {
		return encodeStructured(out, actorDetailExport{
			Actor:     a,
			Phase:     phase,
			Sessions:  sessions,
			SubAgents: children,
		}, outputFormat)
	}

	class := a.Class
	if class == "" {
		if a.IsAgent {
			class = "agent"
		} else {
			class = "shared"
		}
	}
	fmt.Fprintf(out, "Name:               %s\n", a.Name)
	fmt.Fprintf(out, "Atespace:           %s\n", a.Atespace)
	if phase != "" {
		fmt.Fprintf(out, "Task Phase:         %s\n", phase)
	}
	if a.UID != "" {
		fmt.Fprintf(out, "Actor UID:          %s (gen_ai.agent.id)\n", a.UID)
	}
	if a.Template != "" {
		fmt.Fprintf(out, "Template:           %s (ate.template.name)\n", a.Template)
	}
	fmt.Fprintf(out, "Class:              %s (is_agent=%t)\n", class, a.IsAgent)
	if a.Tenant != "" {
		fmt.Fprintf(out, "Tenant:             %s (ate.finops.tenant)\n", a.Tenant)
	}
	if a.ParentAgentID != "" {
		fmt.Fprintf(out, "Parent Agent UID:   %s (gen_ai.agent.parent_id)\n", a.ParentAgentID)
	}
	fmt.Fprintf(out, "Sub-Agents:         %d\n", a.SubAgents)

	printActorCostSections(out, a, sessions, children)
	return nil
}

func printActorCostSections(out io.Writer, a *finops.ActorCost, sessions []finops.SessionCost, children []finops.ActorCost) {
	bookedCompute := a.Compute.Cost.USD()
	measuredCompute := a.MeasuredComputeCost.USD()
	if measuredCompute == 0 {
		measuredCompute = a.Compute.MeasuredCost.USD()
	}
	storageUSD := a.StorageCost.USD()
	egressUSD := a.EgressCost.USD()
	varBooked := a.VariableHostingUSD()
	varMeasured := a.MeasuredVariableHostingUSD()

	idleUSD := a.IdleShareCost.USD()
	platformUSD := a.PlatformShareCost.USD()
	sharedUSD := a.SharedOverheadUSD()

	hostingBooked := a.HostingCost.USD()
	if hostingBooked == 0 {
		hostingBooked = varBooked + sharedUSD
	}
	hostingMeasured := a.MeasuredHostingCost.USD()
	if hostingMeasured == 0 {
		hostingMeasured = varMeasured + sharedUSD
	}

	fmt.Fprintln(out, "\nHosting Cost Breakdown (Three-Number Attribution):")
	hw := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	fmt.Fprintln(hw, "  COMPONENT\tBOOKED-SHARE ($)\tMEASURED-USAGE ($)\tDETAILS")
	fmt.Fprintf(hw, "  1. Variable Hosting Cost\t%s\t%s\tcompute + snapshot storage + egress\n",
		formatUSD(varBooked), formatUSD(varMeasured))
	fmt.Fprintf(hw, "     - Compute Leases\t%s\t%s\tleases=%d duration=%.1fs booked=%.4f vCPU-h / %.4f GiB-h measured=%.2f CPU-s\n",
		formatUSD(bookedCompute), formatUSD(measuredCompute),
		a.Compute.Leases, a.Compute.LeaseSeconds,
		a.Compute.VCPUSeconds/3600.0, a.Compute.GiBSeconds/3600.0,
		a.Compute.MeasuredCPUSeconds)
	fmt.Fprintf(hw, "     - Snapshot Storage\t%s\t%s\tGCS / snapshot GiB-month share\n",
		formatUSD(storageUSD), formatUSD(storageUSD))
	if egressUSD > 0 {
		fmt.Fprintf(hw, "     - Egress Network\t%s\t%s\tmetered internet egress\n",
			formatUSD(egressUSD), formatUSD(egressUSD))
	}
	fmt.Fprintf(hw, "  2. Shared Overhead\t%s\t%s\twarm-pool idle + control-plane share\n",
		formatUSD(sharedUSD), formatUSD(sharedUSD))
	fmt.Fprintf(hw, "     - Warm-Pool Idle Share\t%s\t%s\tunallocated worker pool capacity\n",
		formatUSD(idleUSD), formatUSD(idleUSD))
	fmt.Fprintf(hw, "     - Platform Overhead Share\t%s\t%s\tate-system + finops control plane\n",
		formatUSD(platformUSD), formatUSD(platformUSD))
	fmt.Fprintf(hw, "  3. Fully Loaded Hosting Cost\t%s\t%s\tvariable hosting + shared overhead\n",
		formatUSD(hostingBooked), formatUSD(hostingMeasured))
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
	if ownDirect == 0 {
		ownDirect = a.Cost.USD() + varBooked
	}
	ownLoaded := a.FullyLoadedCost.USD()
	if ownLoaded == 0 {
		ownLoaded = a.Cost.USD() + hostingBooked
	}
	ownMeasuredLoaded := a.MeasuredFullyLoadedCost.USD()
	if ownMeasuredLoaded == 0 {
		ownMeasuredLoaded = a.Cost.USD() + hostingMeasured
	}
	treeLLM := a.TreeLLMCost.USD()
	if treeLLM == 0 {
		treeLLM = a.Cost.USD()
	}
	treeDirect := a.TreeTotalCost.USD()
	if treeDirect == 0 {
		treeDirect = ownDirect
	}
	treeLoaded := a.TreeFullyLoadedCost.USD()
	if treeLoaded == 0 {
		treeLoaded = ownLoaded
	}

	fmt.Fprintln(out, "\nTotal Cost Summary (Own vs. Sub-Agent Tree):")
	tw := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	fmt.Fprintln(tw, "  SCOPE\tLLM ($)\tDIRECT TOTAL ($)\tFULLY LOADED (BOOKED $)\tFULLY LOADED (MEASURED $)")
	fmt.Fprintf(tw, "  Own (%s/%s)\t%s\t%s\t%s\t%s\n",
		a.Atespace, a.Name, formatUSD(a.Cost.USD()), formatUSD(ownDirect), formatUSD(ownLoaded), formatUSD(ownMeasuredLoaded))
	if a.SubAgents > 0 || len(children) > 0 || treeLoaded > ownLoaded {
		fmt.Fprintf(tw, "  Tree (Own + %d Sub-Agents)\t%s\t%s\t%s\t-\n",
			a.SubAgents, formatUSD(treeLLM), formatUSD(treeDirect), formatUSD(treeLoaded))
	}
	_ = tw.Flush()

	if len(children) > 0 {
		fmt.Fprintln(out, "\nSub-Agents (gen_ai.agent.parent_id == Actor UID):")
		cw := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
		fmt.Fprintln(cw, "  NAME\tATESPACE\tUID\tTEMPLATE\tCALLS\tLLM ($)\tHOSTING ($)\tFULLY LOADED ($)")
		for _, c := range children {
			fmt.Fprintf(cw, "  %s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\n",
				c.Name, c.Atespace, c.UID, c.Template, c.Calls,
				formatUSD(c.Cost.USD()), formatUSD(c.HostingCost.USD()), formatUSD(c.FullyLoadedCost.USD()))
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

func renderCurveReport(out io.Writer, curve *finops.CurveReport, outputFormat string) error {
	if outputFormat == "json" || outputFormat == "yaml" {
		return encodeStructured(out, curve, outputFormat)
	}

	fmt.Fprintf(out, "Hosting Cost Sensitivity Curve (< $%.2f / agent-month claim)\n", curve.TargetUSD)
	fmt.Fprintf(out, "  Pool Utilization:      %.0f%%\n", curve.PoolUtilization*100.0)
	fmt.Fprintf(out, "  Storage / Agent-Mo:    %s\n", formatUSD(curve.StoragePerAgentUSD))
	fmt.Fprintf(out, "  Platform / Agent-Mo:   %s\n\n", formatUSD(curve.PlatformPerAgentUSD))

	tw := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	fmt.Fprintln(tw, "TIER\tVARIABLE ($/ACT-HR)\tLOADED ($/ACT-HR)\tFIXED ($/MO)\tMAX HOURS (< TARGET)\tALWAYS-ON (730H)")
	for _, t := range curve.Tiers {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%.2f h/mo (%.2f h/day)\t$%.2f/mo\n",
			t.Tier,
			formatUSD(t.PerActiveHour),
			formatUSD(t.LoadedPerActiveHour),
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
		fmt.Fprintln(out, "\nFully Loaded Hosting Cost ($ / agent-month) by Active Hours:")
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
					row.WriteString(fmt.Sprintf("\t$%.4f", pt.FullyLoaded))
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

func renderFocusReport(out io.Writer, focus *finops.FocusReport, outputFormat string) error {
	if outputFormat == "json" || outputFormat == "yaml" {
		return encodeStructured(out, focus, outputFormat)
	}

	oc := focus.OriginCharge
	fmt.Fprintf(out, "FOCUS 1.3 Split Cost Allocation Report\n")
	if oc.OriginChargeID != "" {
		fmt.Fprintf(out, "  Origin Charge:   %s (%s, resource=%s, billed=%s %s)\n",
			oc.OriginChargeID, oc.ServiceName, oc.ResourceName, formatUSD(oc.BilledCost), oc.BillingCurrency)
	}
	if r := focus.Reconciliation; r != nil {
		status := "OK"
		if len(r.Problems) > 0 {
			status = "PROBLEMS: " + strings.Join(r.Problems, "; ")
		}
		fmt.Fprintf(out, "  Reconciliation:  origin=%s allocated=%s unallocated(_unused_capacity)=%s agents=%d unsized_leases=%d status=%s\n\n",
			formatUSD(r.OriginCost), formatUSD(r.Allocated), formatUSD(r.Unallocated),
			r.Agents, r.UnsizedLeases, status)
	} else {
		fmt.Fprintln(out)
	}

	w := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "ALLOCATED-RESOURCE-ID\tALLOCATED-NAME\tMETHOD\tSERVICE\tQUANTITY\tUNIT\tBILLED ($)\tTENANT\tCLASS")
	for _, c := range focus.Charges {
		resID := c.AllocatedResourceID
		if resID == "" {
			resID = "-"
		}
		resName := c.AllocatedResourceName
		if resName == "" {
			resName = "-"
		}
		tenant := firstNonEmpty(
			c.AllocatedTags["tenant"],
			c.AllocatedTags["finops.agent-substrate.dev/tenant"],
			c.AllocatedTags["ate.finops.tenant"],
			"-",
		)
		class := firstNonEmpty(
			c.AllocatedTags["finops.agent-substrate.dev/class"],
			c.AllocatedTags["ate.template.class"],
			c.AllocatedTags["class"],
		)
		if class == "" {
			if c.AllocatedTags["is_agent"] == "true" || c.AllocatedTags["finops.agent-substrate.dev/is_agent"] == "true" {
				class = "agent"
			} else if c.AllocatedTags["is_agent"] == "false" || c.AllocatedTags["finops.agent-substrate.dev/is_agent"] == "false" {
				class = "shared"
			} else {
				class = "-"
			}
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%.4f\t%s\t%s\t%s\t%s\n",
			resID, resName, c.AllocatedMethodID, c.ServiceName,
			c.ConsumedQuantity, c.ConsumedUnit, formatUSD(c.BilledCost), tenant, class)
	}
	return w.Flush()
}

func encodeStructured(out io.Writer, v any, format string) error {
	switch format {
	case "json":
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	case "yaml":
		enc := yaml.NewEncoder(out)
		enc.SetIndent(2)
		defer enc.Close()
		return enc.Encode(v)
	default:
		return fmt.Errorf("unsupported structured format %q", format)
	}
}

func formatUSD(v float64) string {
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

// resolveOptionalFinOpsURL returns a FinOps HTTP server URL if one is explicitly
// configured or reachable via an active local tunnel/port-forward.
func resolveOptionalFinOpsURL(finopsServer, kubeContext string) string {
	if finopsServer != "" {
		return finopsServer
	}
	if env := os.Getenv("AX_FINOPS_SERVER"); env != "" {
		return env
	}
	if env := os.Getenv("FINOPS_SERVER"); env != "" {
		return env
	}
	if ctxName, err := tunnel.CurrentContext(kubeContext); err == nil && ctxName != "" {
		if existing, err := tunnel.GetTunnel(ctxName + "-finops"); err == nil && existing != nil && tunnel.IsFinOpsTunnelHealthy(existing.Port) {
			return fmt.Sprintf("http://127.0.0.1:%d", existing.Port)
		}
	}
	for _, p := range []int{18080, 9090} {
		if tunnel.IsFinOpsTunnelHealthy(p) {
			return fmt.Sprintf("http://127.0.0.1:%d", p)
		}
	}
	return ""
}

// tryPrintTaskDescribeCost appends FinOps cost attribution to `ax describe task`
// when a FinOps server is reachable and has records for the task's actor.
func tryPrintTaskDescribeCost(out io.Writer, finopsURL, atespace, taskName, actorName string) {
	if finopsURL == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client := finops.NewClient(finopsURL, nil)
	lookupName := actorName
	if lookupName == "" {
		lookupName = taskName
	}
	actorCost, sessions, children, _, err := client.GetActorCost(ctx, atespace, lookupName)
	if err != nil || actorCost == nil {
		return
	}
	printActorCostSections(out, actorCost, sessions, children)
}

// sortStrings is a tiny helper to keep imports clean if needed.
func sortStrings(s []string) {
	sort.Strings(s)
}
