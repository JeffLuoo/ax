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

// Package finops provides an HTTP client for querying the Agent FinOps cost
// and allocation APIs (/v1/costs, /v1/costs/curve, /v1/costs/focus).
package finops

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultFinOpsNamespace is the default Kubernetes namespace where the FinOps
// service runs.
const DefaultFinOpsNamespace = "ate-finops"

// DefaultFinOpsService is the default Kubernetes Service name for the FinOps
// HTTP API.
const DefaultFinOpsService = "finops"

// DefaultFinOpsPort is the default HTTP port exposed by the FinOps service.
const DefaultFinOpsPort = 9090

// MoneyMap holds currency-keyed amounts (e.g. {"USD": 0.0659125}).
type MoneyMap map[string]float64

// USD returns the USD amount from the MoneyMap.
func (m MoneyMap) USD() float64 {
	if m == nil {
		return 0
	}
	if v, ok := m["USD"]; ok {
		return v
	}
	if v, ok := m["usd"]; ok {
		return v
	}
	return 0
}

// Totals summarizes token and LLM call spend.
type Totals struct {
	Calls                 int64    `json:"calls" yaml:"calls"`
	InputTokens           int64    `json:"inputTokens" yaml:"inputTokens"`
	OutputTokens          int64    `json:"outputTokens" yaml:"outputTokens"`
	CacheReadInputTokens  int64    `json:"cacheReadInputTokens,omitempty" yaml:"cacheReadInputTokens,omitempty"`
	CacheWriteInputTokens int64    `json:"cacheWriteInputTokens,omitempty" yaml:"cacheWriteInputTokens,omitempty"`
	ReasoningOutputTokens int64    `json:"reasoningOutputTokens,omitempty" yaml:"reasoningOutputTokens,omitempty"`
	WebSearchRequests     int64    `json:"webSearchRequests,omitempty" yaml:"webSearchRequests,omitempty"`
	Cost                  MoneyMap `json:"cost" yaml:"cost"`
	UnpricedCalls         int64    `json:"unpricedCalls,omitempty" yaml:"unpricedCalls,omitempty"`
	// UnknownCostCalls counts calls whose cost is unknown: no usage report
	// arrived (for example a stream without usage, or one cut short), so
	// they are neither priced nor counted in UnpricedCalls.
	UnknownCostCalls int64            `json:"unknownCostCalls,omitempty" yaml:"unknownCostCalls,omitempty"`
	LowerBoundCalls  int64            `json:"lowerBoundCalls,omitempty" yaml:"lowerBoundCalls,omitempty"`
	UsageStatus      map[string]int64 `json:"usageStatus,omitempty" yaml:"usageStatus,omitempty"`
	LastCall         string           `json:"lastCall,omitempty" yaml:"lastCall,omitempty"`
}

// ComputeSummary summarizes booked and measured compute leases.
type ComputeSummary struct {
	Leases       int64    `json:"leases" yaml:"leases"`
	OpenLeases   int64    `json:"openLeases,omitempty" yaml:"openLeases,omitempty"`
	LeaseSeconds float64  `json:"leaseSeconds" yaml:"leaseSeconds"`
	VCPUSeconds  float64  `json:"vcpuSeconds" yaml:"vcpuSeconds"`
	GiBSeconds   float64  `json:"gibSeconds" yaml:"gibSeconds"`
	Cost         MoneyMap `json:"cost" yaml:"cost"`
	MeasuredCost MoneyMap `json:"measuredCost,omitempty" yaml:"measuredCost,omitempty"`
	// MeasuredFallbackLeases had no usage sample, so their measured cost is
	// the booked cost.
	MeasuredFallbackLeases int64   `json:"measuredFallbackLeases,omitempty" yaml:"measuredFallbackLeases,omitempty"`
	UnpricedLeases         int64   `json:"unpricedLeases,omitempty" yaml:"unpricedLeases,omitempty"`
	MeasuredCPUSeconds     float64 `json:"measuredCpuSeconds,omitempty" yaml:"measuredCpuSeconds,omitempty"`
	LastLease              string  `json:"lastLease,omitempty" yaml:"lastLease,omitempty"`
}

// ModelCost holds per-(provider, model) token usage and spend for an actor.
type ModelCost struct {
	Provider              string           `json:"provider" yaml:"provider"`
	Model                 string           `json:"model" yaml:"model"`
	Calls                 int64            `json:"calls" yaml:"calls"`
	InputTokens           int64            `json:"inputTokens" yaml:"inputTokens"`
	OutputTokens          int64            `json:"outputTokens" yaml:"outputTokens"`
	CacheReadInputTokens  int64            `json:"cacheReadInputTokens,omitempty" yaml:"cacheReadInputTokens,omitempty"`
	CacheWriteInputTokens int64            `json:"cacheWriteInputTokens,omitempty" yaml:"cacheWriteInputTokens,omitempty"`
	ReasoningOutputTokens int64            `json:"reasoningOutputTokens,omitempty" yaml:"reasoningOutputTokens,omitempty"`
	Cost                  MoneyMap         `json:"cost" yaml:"cost"`
	UnpricedCalls         int64            `json:"unpricedCalls,omitempty" yaml:"unpricedCalls,omitempty"`
	UsageStatus           map[string]int64 `json:"usageStatus,omitempty" yaml:"usageStatus,omitempty"`
	LastCall              string           `json:"lastCall,omitempty" yaml:"lastCall,omitempty"`
}

// ActorCost holds active cost attribution (LLM, compute, storage, egress,
// and sub-agent tree rollup) for one actor or Kubernetes pod.
type ActorCost struct {
	Atespace      string `json:"atespace" yaml:"atespace"`
	Name          string `json:"name" yaml:"name"`
	UID           string `json:"uid,omitempty" yaml:"uid,omitempty"`
	Tenant        string `json:"tenant,omitempty" yaml:"tenant,omitempty"`
	ParentAgentID string `json:"parentAgentId,omitempty" yaml:"parentAgentId,omitempty"`
	SubAgents     int    `json:"subAgents,omitempty" yaml:"subAgents,omitempty"`
	Template      string `json:"template,omitempty" yaml:"template,omitempty"`
	Class         string `json:"class,omitempty" yaml:"class,omitempty"`
	IsAgent       bool   `json:"isAgent" yaml:"isAgent"`
	// Note is set on a row that is not one actor: spend whose actor finops
	// could not identify, or actors beyond its limit. Such a row has no name.
	Note                  string           `json:"note,omitempty" yaml:"note,omitempty"`
	Calls                 int64            `json:"calls" yaml:"calls"`
	InputTokens           int64            `json:"inputTokens" yaml:"inputTokens"`
	OutputTokens          int64            `json:"outputTokens" yaml:"outputTokens"`
	CacheReadInputTokens  int64            `json:"cacheReadInputTokens,omitempty" yaml:"cacheReadInputTokens,omitempty"`
	CacheWriteInputTokens int64            `json:"cacheWriteInputTokens,omitempty" yaml:"cacheWriteInputTokens,omitempty"`
	ReasoningOutputTokens int64            `json:"reasoningOutputTokens,omitempty" yaml:"reasoningOutputTokens,omitempty"`
	WebSearchRequests     int64            `json:"webSearchRequests,omitempty" yaml:"webSearchRequests,omitempty"`
	Cost                  MoneyMap         `json:"cost" yaml:"cost"`
	UnpricedCalls         int64            `json:"unpricedCalls,omitempty" yaml:"unpricedCalls,omitempty"`
	UnknownCostCalls      int64            `json:"unknownCostCalls,omitempty" yaml:"unknownCostCalls,omitempty"`
	LowerBoundCalls       int64            `json:"lowerBoundCalls,omitempty" yaml:"lowerBoundCalls,omitempty"`
	UsageStatus           map[string]int64 `json:"usageStatus,omitempty" yaml:"usageStatus,omitempty"`
	LastCall              string           `json:"lastCall,omitempty" yaml:"lastCall,omitempty"`
	Compute               ComputeSummary   `json:"compute" yaml:"compute"`
	TotalCost             MoneyMap         `json:"totalCost" yaml:"totalCost"`
	MeasuredComputeCost   MoneyMap         `json:"measuredComputeCost,omitempty" yaml:"measuredComputeCost,omitempty"`
	StorageCost           MoneyMap         `json:"storageCost,omitempty" yaml:"storageCost,omitempty"`
	EgressCost            MoneyMap         `json:"egressCost,omitempty" yaml:"egressCost,omitempty"`
	HostingCost           MoneyMap         `json:"hostingCost,omitempty" yaml:"hostingCost,omitempty"`
	MeasuredHostingCost   MoneyMap         `json:"measuredHostingCost,omitempty" yaml:"measuredHostingCost,omitempty"`
	TreeLLMCost           MoneyMap         `json:"treeLlmCost,omitempty" yaml:"treeLlmCost,omitempty"`
	TreeComputeCost       MoneyMap         `json:"treeComputeCost,omitempty" yaml:"treeComputeCost,omitempty"`
	TreeTotalCost         MoneyMap         `json:"treeTotalCost,omitempty" yaml:"treeTotalCost,omitempty"`
	// Task is the Ax task that the actor runs, when finops knows it.
	Task   *TaskCost   `json:"task,omitempty" yaml:"task,omitempty"`
	Models []ModelCost `json:"models,omitempty" yaml:"models,omitempty"`
}

// TaskCost is the Ax task that an actor runs, with the actor's spend against
// the task's budget, as finops reports it. Spend is LLM plus compute, in USD.
type TaskCost struct {
	Name           string  `json:"name" yaml:"name"`
	Parent         string  `json:"parent,omitempty" yaml:"parent,omitempty"`
	ParentSource   string  `json:"parentSource,omitempty" yaml:"parentSource,omitempty"`
	BudgetUSD      float64 `json:"budgetUsd,omitempty" yaml:"budgetUsd,omitempty"`
	BudgetSource   string  `json:"budgetSource,omitempty" yaml:"budgetSource,omitempty"`
	SpendUSD       float64 `json:"spendUsd" yaml:"spendUsd"`
	TreeSpendUSD   float64 `json:"treeSpendUsd" yaml:"treeSpendUsd"`
	OverBudget     bool    `json:"overBudget" yaml:"overBudget"`
	TreeOverBudget bool    `json:"treeOverBudget" yaml:"treeOverBudget"`
	Outcome        string  `json:"outcome,omitempty" yaml:"outcome,omitempty"`
	ExitCode       *int32  `json:"exitCode,omitempty" yaml:"exitCode,omitempty"`
	// OutcomeSource says who decided Outcome: "sandbox" (the task's own exit
	// code, so self-reported), "orchestrator", or "external". Servers before
	// it existed send none; treat that as "sandbox".
	OutcomeSource string `json:"outcomeSource,omitempty" yaml:"outcomeSource,omitempty"`
}

// OutcomeTotals is the cost of the actors whose tasks had one outcome.
type OutcomeTotals struct {
	Tasks     int      `json:"tasks" yaml:"tasks"`
	Actors    int      `json:"actors" yaml:"actors"`
	TotalCost MoneyMap `json:"totalCost" yaml:"totalCost"`
}

// OutcomeCosts splits the actors' own cost by their task's outcome.
type OutcomeCosts struct {
	Completed  OutcomeTotals `json:"completed" yaml:"completed"`
	Failed     OutcomeTotals `json:"failed" yaml:"failed"`
	Unfinished OutcomeTotals `json:"unfinished" yaml:"unfinished"`
	NoTask     OutcomeTotals `json:"noTask" yaml:"noTask"`
	// OutcomeSource is "sandbox" when any Completed or Failed outcome is
	// self-reported, else the one source they share, else "mixed".
	OutcomeSource string `json:"outcomeSource,omitempty" yaml:"outcomeSource,omitempty"`
}

// VariableHostingUSD returns the active hosting cost (booked compute + storage + egress).
func (a ActorCost) VariableHostingUSD() float64 {
	return a.Compute.Cost.USD() + a.StorageCost.USD() + a.EgressCost.USD()
}

// MeasuredVariableHostingUSD returns the active hosting cost using measured CPU usage.
func (a ActorCost) MeasuredVariableHostingUSD() float64 {
	mc := a.MeasuredComputeCost.USD()
	if mc == 0 {
		mc = a.Compute.MeasuredCost.USD()
	}
	return mc + a.StorageCost.USD() + a.EgressCost.USD()
}

// TemplateCost summarizes cost by agent/actor template.
type TemplateCost struct {
	Template  string         `json:"template" yaml:"template"`
	Class     string         `json:"class" yaml:"class"`
	Actors    int            `json:"actors" yaml:"actors"`
	LLMCost   MoneyMap       `json:"llmCost" yaml:"llmCost"`
	Compute   ComputeSummary `json:"compute" yaml:"compute"`
	TotalCost MoneyMap       `json:"totalCost" yaml:"totalCost"`
}

// SessionCost summarizes cost per conversation session or activation epoch.
type SessionCost struct {
	SessionID     string   `json:"sessionId" yaml:"sessionId"`
	FromHeader    bool     `json:"fromHeader" yaml:"fromHeader"`
	Atespace      string   `json:"atespace" yaml:"atespace"`
	Actor         string   `json:"actor" yaml:"actor"`
	ActorUID      string   `json:"actorUid,omitempty" yaml:"actorUid,omitempty"`
	Epoch         string   `json:"epoch,omitempty" yaml:"epoch,omitempty"`
	Calls         int64    `json:"calls" yaml:"calls"`
	InputTokens   int64    `json:"inputTokens" yaml:"inputTokens"`
	OutputTokens  int64    `json:"outputTokens" yaml:"outputTokens"`
	ActiveSeconds float64  `json:"activeSeconds" yaml:"activeSeconds"`
	ActiveShare   float64  `json:"activeShare" yaml:"activeShare"`
	SplitBasis    string   `json:"splitBasis,omitempty" yaml:"splitBasis,omitempty"`
	LLMCost       MoneyMap `json:"llmCost" yaml:"llmCost"`
	ComputeCost   MoneyMap `json:"computeCost" yaml:"computeCost"`
	TotalCost     MoneyMap `json:"totalCost" yaml:"totalCost"`
	LastActivity  string   `json:"lastActivity,omitempty" yaml:"lastActivity,omitempty"`
}

// CurvePoint represents a single active-hours point on the hosting cost curve.
type CurvePoint struct {
	ActiveHours float64 `json:"activeHours" yaml:"activeHours"`
	Variable    float64 `json:"variable" yaml:"variable"`
	Egress      float64 `json:"egress,omitempty" yaml:"egress,omitempty"`
	Storage     float64 `json:"storage" yaml:"storage"`
	ActiveCost  float64 `json:"activeCost" yaml:"activeCost"`
	UnderTarget bool    `json:"underTarget" yaml:"underTarget"`
}

// TierCurve holds the hosting cost sensitivity curve for one pricing tier.
type TierCurve struct {
	Tier           string       `json:"tier" yaml:"tier"`
	PerActiveHour  float64      `json:"perActiveHour" yaml:"perActiveHour"`
	EgressPerHour  float64      `json:"egressPerHour,omitempty" yaml:"egressPerHour,omitempty"`
	Fixed          float64      `json:"fixed" yaml:"fixed"`
	BreakEvenHours float64      `json:"breakEvenHours" yaml:"breakEvenHours"`
	AlwaysOn       float64      `json:"alwaysOn" yaml:"alwaysOn"`
	Points         []CurvePoint `json:"points" yaml:"points"`
}

// CurveReport is the response from GET /v1/costs/curve (and embedded in /v1/costs).
type CurveReport struct {
	TargetUSD          float64     `json:"targetUsd" yaml:"targetUsd"`
	StoragePerAgentUSD float64     `json:"storagePerAgentUsd" yaml:"storagePerAgentUsd"`
	PriceList          string      `json:"priceList,omitempty" yaml:"priceList,omitempty"`
	Tiers              []TierCurve `json:"tiers" yaml:"tiers"`
}

// FocusElement describes one resource element in FOCUS AllocatedMethodDetails.
type FocusElement struct {
	AllocatedRatio           float64 `json:"AllocatedRatio,omitempty" yaml:"AllocatedRatio,omitempty"`
	UsageQuantity            float64 `json:"UsageQuantity,omitempty" yaml:"UsageQuantity,omitempty"`
	UsageUnit                string  `json:"UsageUnit,omitempty" yaml:"UsageUnit,omitempty"`
	PricePerUnit             float64 `json:"x_PricePerUnit,omitempty" yaml:"x_PricePerUnit,omitempty"`
	UnusedCapacityResourceID string  `json:"x_UnusedCapacityResourceId,omitempty" yaml:"x_UnusedCapacityResourceId,omitempty"`
	// Method is the method that produced the row's numbers, which can
	// differ from AllocatedMethodId when usage samples are missing.
	Method               string   `json:"x_Method,omitempty" yaml:"x_Method,omitempty"`
	MeasuredLeases       int64    `json:"x_MeasuredLeases,omitempty" yaml:"x_MeasuredLeases,omitempty"`
	BookedFallbackLeases int64    `json:"x_BookedFallbackLeases,omitempty" yaml:"x_BookedFallbackLeases,omitempty"`
	IdleVCPUHours        *float64 `json:"x_IdleVCPUHours,omitempty" yaml:"x_IdleVCPUHours,omitempty"`
	IdleGiBHours         *float64 `json:"x_IdleGiBHours,omitempty" yaml:"x_IdleGiBHours,omitempty"`
	ExpectedIdleCost     *float64 `json:"x_ExpectedIdleCost,omitempty" yaml:"x_ExpectedIdleCost,omitempty"`
	PriceGap             *float64 `json:"x_PriceGap,omitempty" yaml:"x_PriceGap,omitempty"`
}

// FocusMethodDetails holds FOCUS 1.3 AllocatedMethodDetails.
type FocusMethodDetails struct {
	Elements []FocusElement `json:"Elements,omitempty" yaml:"Elements,omitempty"`
}

// EstimatedTag is the FOCUS tag that finops sets to "true" on a charge that
// comes from rate card prices rather than from the bill.
const EstimatedTag = "finops.agent-substrate.dev/estimated"

// FocusCharge represents one FOCUS 1.3 split cost allocation row.
type FocusCharge struct {
	OriginChargeID         string             `json:"OriginChargeId" yaml:"OriginChargeId"`
	BillingAccountID       string             `json:"BillingAccountId,omitempty" yaml:"BillingAccountId,omitempty"`
	BillingCurrency        string             `json:"BillingCurrency" yaml:"BillingCurrency"`
	ChargeClass            string             `json:"ChargeClass,omitempty" yaml:"ChargeClass,omitempty"`
	RegionID               string             `json:"RegionId,omitempty" yaml:"RegionId,omitempty"`
	Tags                   map[string]string  `json:"Tags,omitempty" yaml:"Tags,omitempty"`
	BillingPeriodStart     string             `json:"BillingPeriodStart,omitempty" yaml:"BillingPeriodStart,omitempty"`
	BillingPeriodEnd       string             `json:"BillingPeriodEnd,omitempty" yaml:"BillingPeriodEnd,omitempty"`
	ChargePeriodStart      string             `json:"ChargePeriodStart,omitempty" yaml:"ChargePeriodStart,omitempty"`
	ChargePeriodEnd        string             `json:"ChargePeriodEnd,omitempty" yaml:"ChargePeriodEnd,omitempty"`
	ChargeCategory         string             `json:"ChargeCategory,omitempty" yaml:"ChargeCategory,omitempty"`
	ChargeDescription      string             `json:"ChargeDescription,omitempty" yaml:"ChargeDescription,omitempty"`
	ServiceProviderName    string             `json:"ServiceProviderName,omitempty" yaml:"ServiceProviderName,omitempty"`
	HostProviderName       string             `json:"HostProviderName,omitempty" yaml:"HostProviderName,omitempty"`
	InvoiceIssuerName      string             `json:"InvoiceIssuerName,omitempty" yaml:"InvoiceIssuerName,omitempty"`
	ServiceName            string             `json:"ServiceName,omitempty" yaml:"ServiceName,omitempty"`
	ServiceCategory        string             `json:"ServiceCategory,omitempty" yaml:"ServiceCategory,omitempty"`
	ResourceID             string             `json:"ResourceId,omitempty" yaml:"ResourceId,omitempty"`
	ResourceName           string             `json:"ResourceName,omitempty" yaml:"ResourceName,omitempty"`
	ResourceType           string             `json:"ResourceType,omitempty" yaml:"ResourceType,omitempty"`
	PricingUnit            string             `json:"PricingUnit,omitempty" yaml:"PricingUnit,omitempty"`
	PricingQuantity        float64            `json:"PricingQuantity,omitempty" yaml:"PricingQuantity,omitempty"`
	ConsumedUnit           string             `json:"ConsumedUnit,omitempty" yaml:"ConsumedUnit,omitempty"`
	ConsumedQuantity       float64            `json:"ConsumedQuantity,omitempty" yaml:"ConsumedQuantity,omitempty"`
	ListCost               float64            `json:"ListCost" yaml:"ListCost"`
	ContractedCost         float64            `json:"ContractedCost" yaml:"ContractedCost"`
	EffectiveCost          float64            `json:"EffectiveCost" yaml:"EffectiveCost"`
	BilledCost             float64            `json:"BilledCost" yaml:"BilledCost"`
	AllocatedMethodID      string             `json:"AllocatedMethodId,omitempty" yaml:"AllocatedMethodId,omitempty"`
	AllocatedMethodDetails FocusMethodDetails `json:"AllocatedMethodDetails,omitempty" yaml:"AllocatedMethodDetails,omitempty"`
	AllocatedResourceID    string             `json:"AllocatedResourceId,omitempty" yaml:"AllocatedResourceId,omitempty"`
	AllocatedResourceName  string             `json:"AllocatedResourceName,omitempty" yaml:"AllocatedResourceName,omitempty"`
	AllocatedTags          map[string]string  `json:"AllocatedTags,omitempty" yaml:"AllocatedTags,omitempty"`
}

// Estimated reports whether the charge is an estimate from rate card prices.
func (c FocusCharge) Estimated() bool {
	return c.Tags[EstimatedTag] == "true" || c.AllocatedTags[EstimatedTag] == "true"
}

// FocusReconciliation summarizes bottom-up vs. origin charge reconciliation.
type FocusReconciliation struct {
	ResourceID    string  `json:"resourceId" yaml:"resourceId"`
	Pool          string  `json:"pool" yaml:"pool"`
	PeriodStart   string  `json:"periodStart" yaml:"periodStart"`
	PeriodEnd     string  `json:"periodEnd" yaml:"periodEnd"`
	Estimated     bool    `json:"estimated" yaml:"estimated"`
	OriginCost    float64 `json:"originCost" yaml:"originCost"`
	Allocated     float64 `json:"allocated" yaml:"allocated"`
	Unallocated   float64 `json:"unallocated" yaml:"unallocated"`
	Agents        int     `json:"agents" yaml:"agents"`
	UnsizedLeases int     `json:"unsizedLeases" yaml:"unsizedLeases"`
	// EstimatedLeases had a start or end that no lifecycle record showed.
	EstimatedLeases int      `json:"estimatedLeases,omitempty" yaml:"estimatedLeases,omitempty"`
	Problems        []string `json:"problems,omitempty" yaml:"problems,omitempty"`
	// ExpectedIdle is the idle capacity at rate card prices, when the pool's
	// capacity is known.
	ExpectedIdle *float64 `json:"expectedIdle,omitempty" yaml:"expectedIdle,omitempty"`
	// PriceGap compares the bill with capacity at rate card prices; it does
	// not depend on the leases. Older servers send it as Gap.
	PriceGap *float64 `json:"priceGap,omitempty" yaml:"priceGap,omitempty"`
	Gap      *float64 `json:"gap,omitempty" yaml:"gap,omitempty"`
	// The coverage check compares the leases' hours with the activation
	// hours in the runtime's usage records, which come from another source.
	CoverageLeaseHours      *float64 `json:"coverageLeaseHours,omitempty" yaml:"coverageLeaseHours,omitempty"`
	CoverageActivationHours *float64 `json:"coverageActivationHours,omitempty" yaml:"coverageActivationHours,omitempty"`
	CoverageCoveredHours    *float64 `json:"coverageCoveredHours,omitempty" yaml:"coverageCoveredHours,omitempty"`
	CoverageExcessHours     *float64 `json:"coverageExcessHours,omitempty" yaml:"coverageExcessHours,omitempty"`
	CoverageRatio           *float64 `json:"coverageRatio,omitempty" yaml:"coverageRatio,omitempty"`
}

// PriceGapValue returns PriceGap, or Gap from an older server, or nil.
func (r FocusReconciliation) PriceGapValue() *float64 {
	if r.PriceGap != nil {
		return r.PriceGap
	}
	return r.Gap
}

// FocusReport is the response from GET /v1/costs/focus.
type FocusReport struct {
	// OriginCharge is the first of OriginCharges, one per pool, and
	// Reconciliation totals Reconciliations.
	OriginCharge     FocusCharge           `json:"originCharge" yaml:"originCharge"`
	OriginCharges    []FocusCharge         `json:"originCharges,omitempty" yaml:"originCharges,omitempty"`
	Charges          []FocusCharge         `json:"charges" yaml:"charges"`
	AuxiliaryCharges []FocusCharge         `json:"auxiliaryCharges,omitempty" yaml:"auxiliaryCharges,omitempty"`
	Reconciliation   *FocusReconciliation  `json:"reconciliation,omitempty" yaml:"reconciliation,omitempty"`
	Reconciliations  []FocusReconciliation `json:"reconciliations,omitempty" yaml:"reconciliations,omitempty"`
}

// Estimated reports whether the report's origin is an estimate from rate
// card prices rather than a bill.
func (f FocusReport) Estimated() bool {
	return (f.Reconciliation != nil && f.Reconciliation.Estimated) || f.OriginCharge.Estimated()
}

// CostBasisEstimated is the CostReport.CostBasis of numbers that come from
// rate card prices and have not been reconciled with a bill.
const CostBasisEstimated = "estimated"

// CostReport is the response from GET /v1/costs. The CLI reads it to render
// tables; -o json and -o yaml print the server's body as it came.
type CostReport struct {
	Since       string `json:"since" yaml:"since"`
	GeneratedAt string `json:"generatedAt" yaml:"generatedAt"`
	// Atespace is set when the server filtered the report to one atespace;
	// its totals then cover only that atespace.
	Atespace string `json:"atespace,omitempty" yaml:"atespace,omitempty"`
	// CostBasis says how far the numbers can be trusted, e.g. "estimated".
	CostBasis              string               `json:"costBasis,omitempty" yaml:"costBasis,omitempty"`
	Totals                 Totals               `json:"totals" yaml:"totals"`
	Compute                ComputeSummary       `json:"compute" yaml:"compute"`
	TotalCost              MoneyMap             `json:"totalCost" yaml:"totalCost"`
	Actors                 []ActorCost          `json:"actors" yaml:"actors"`
	Templates              []TemplateCost       `json:"templates,omitempty" yaml:"templates,omitempty"`
	Sessions               []SessionCost        `json:"sessions,omitempty" yaml:"sessions,omitempty"`
	Curve                  []TierCurve          `json:"curve,omitempty" yaml:"curve,omitempty"`
	Focus                  []FocusCharge        `json:"focus,omitempty" yaml:"focus,omitempty"`
	FocusReconciliation    *FocusReconciliation `json:"focusReconciliation,omitempty" yaml:"focusReconciliation,omitempty"`
	CrossTenantParentLinks int                  `json:"crossTenantParentLinks,omitempty" yaml:"crossTenantParentLinks,omitempty"`
	OutcomeCosts           *OutcomeCosts        `json:"outcomeCosts,omitempty" yaml:"outcomeCosts,omitempty"`
}

// Estimated reports whether the report's numbers are estimates: the server
// says so, or it says nothing (finops prices from a rate card unless a bill
// is reconciled).
func (r CostReport) Estimated() bool {
	return r.CostBasis == "" || r.CostBasis == CostBasisEstimated ||
		(r.FocusReconciliation != nil && r.FocusReconciliation.Estimated)
}

// Client queries the FinOps HTTP server.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a new FinOps HTTP client targeting baseURL.
func NewClient(baseURL string, httpClient *http.Client) *Client {
	u := strings.TrimSpace(baseURL)
	if u == "" {
		u = fmt.Sprintf("http://127.0.0.1:%d", DefaultFinOpsPort)
	}
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		u = "http://" + u
	}
	u = strings.TrimRight(u, "/")
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{
		baseURL:    u,
		httpClient: httpClient,
	}
}

// BaseURL returns the normalized base URL of the FinOps server.
func (c *Client) BaseURL() string {
	return c.baseURL
}

// GetCosts fetches GET /v1/costs. A non-empty atespace asks the server for
// that atespace only, so the totals cover only its actors. It returns the
// decoded report and the body as the server sent it.
func (c *Client) GetCosts(ctx context.Context, atespace string) (*CostReport, []byte, error) {
	var report CostReport
	raw, err := c.getJSON(ctx, "/v1/costs", atespaceQuery(atespace), &report)
	if err != nil {
		return nil, nil, err
	}
	return &report, raw, nil
}

// ActorMatch is one actor's cost record with the rows that belong to it.
type ActorMatch struct {
	Actor *ActorCost
	// Raw is the actor's row as the server sent it.
	Raw       json.RawMessage
	Sessions  []json.RawMessage
	SubAgents []json.RawMessage
	// SessionCosts and Children are the decoded Sessions and SubAgents.
	SessionCosts []SessionCost
	Children     []ActorCost
	// RawActors holds the raw actor rows of Report.Actors so the caller can
	// populate SubAgents from its own task tree.
	RawActors []json.RawMessage
	// Canonical reports whether Actor is the canonical cost record for its
	// "<atespace>/<name>".
	Canonical bool
	Report    *CostReport
}

// NotFoundError is returned when GetActorCost finds no FinOps record for the
// target actor.
type NotFoundError struct {
	Target string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("no FinOps cost record found for %q", e.Target)
}

// AmbiguousError is returned when a name or UID prefix matches more than one
// actor record.
type AmbiguousError struct {
	Target     string
	Candidates []string
}

func (e *AmbiguousError) Error() string {
	return fmt.Sprintf("%q matches %d FinOps cost records (%s); name one by its full actor UID", e.Target, len(e.Candidates), strings.Join(e.Candidates, ", "))
}

// MinUIDPrefix is the shortest UID prefix that GetActorCost accepts.
const MinUIDPrefix = 8

// GetActorCost finds one actor's cost record. target is a name, an
// "<atespace>/<name>", an actor UID (gen_ai.agent.id), or a unique UID
// prefix of at least MinUIDPrefix characters.
//
// A name matches only exactly. When atespaceExplicit is true, or target names
// its atespace, only that atespace is searched. Otherwise the atespace is
// searched first and then all atespaces. More than one match, for example an
// actor that was made again under the same name with a new UID, is an
// *AmbiguousError.
func (c *Client) GetActorCost(ctx context.Context, atespace string, atespaceExplicit bool, target string) (*ActorMatch, error) {
	target = strings.TrimSpace(target)
	if parts := strings.SplitN(target, "/", 2); len(parts) == 2 {
		atespace, target, atespaceExplicit = parts[0], parts[1], true
	}
	scope := ""
	if atespaceExplicit {
		scope = atespace
	}
	report, raw, err := c.GetCosts(ctx, scope)
	if err != nil {
		return nil, err
	}
	var rawReport struct {
		Actors   []json.RawMessage `json:"actors"`
		Sessions []json.RawMessage `json:"sessions"`
	}
	if err := json.Unmarshal(raw, &rawReport); err != nil || len(rawReport.Actors) != len(report.Actors) || len(rawReport.Sessions) != len(report.Sessions) {
		return nil, fmt.Errorf("decoding the FinOps cost report rows: %v", err)
	}

	inScope := func(a ActorCost) bool { return !atespaceExplicit || a.Atespace == atespace }
	byName := func(onlyAtespace string) []int {
		var out []int
		for i, a := range report.Actors {
			if a.Note == "" && a.Name == target && (onlyAtespace == "" || a.Atespace == onlyAtespace) {
				out = append(out, i)
			}
		}
		return out
	}

	matches := byName(atespace)
	if len(matches) == 0 && !atespaceExplicit {
		matches = byName("")
	}
	if len(matches) == 0 {
		for i, a := range report.Actors {
			if a.UID != "" && a.UID == target && inScope(a) {
				matches = append(matches, i)
			}
		}
	}
	if len(matches) == 0 && len(target) >= MinUIDPrefix {
		for i, a := range report.Actors {
			if a.UID != "" && strings.HasPrefix(a.UID, target) && inScope(a) {
				matches = append(matches, i)
			}
		}
	}
	switch len(matches) {
	case 0:
		where := target
		if atespaceExplicit {
			where = atespace + "/" + target
		}
		return nil, &NotFoundError{Target: where}
	case 1:
	default:
		var cands []string
		for _, i := range matches {
			a := report.Actors[i]
			cands = append(cands, fmt.Sprintf("%s/%s uid=%s", a.Atespace, a.Name, a.UID))
		}
		return nil, &AmbiguousError{Target: target, Candidates: cands}
	}

	idx := matches[0]
	matched := &report.Actors[idx]
	canonicalIdx := CanonicalActorIndex(report.Actors, matched.Atespace, matched.Name)
	isCanonical := idx == canonicalIdx || canonicalIdx == -1
	m := &ActorMatch{
		Actor:     matched,
		Raw:       rawReport.Actors[idx],
		RawActors: rawReport.Actors,
		Canonical: isCanonical,
		Report:    report,
	}
	fullName := matched.Atespace + "/" + matched.Name
	for i, s := range report.Sessions {
		mine := s.ActorUID != "" && s.ActorUID == matched.UID
		if s.ActorUID == "" || matched.UID == "" {
			mine = isCanonical && s.Actor == fullName && (s.Atespace == "" || s.Atespace == matched.Atespace)
		}
		if mine {
			m.Sessions = append(m.Sessions, rawReport.Sessions[i])
			m.SessionCosts = append(m.SessionCosts, s)
		}
	}
	return m, nil
}

// GetCurve fetches the < $1/agent-month hosting cost sensitivity curve from
// GET /v1/costs/curve, decoded and as the server sent it.
func (c *Client) GetCurve(ctx context.Context) (*CurveReport, []byte, error) {
	var curve CurveReport
	raw, err := c.getJSON(ctx, "/v1/costs/curve", nil, &curve)
	if err != nil {
		return nil, nil, err
	}
	return &curve, raw, nil
}

// GetFocus fetches the FOCUS 1.3 split cost allocation report from
// GET /v1/costs/focus, decoded and as the server sent it.
func (c *Client) GetFocus(ctx context.Context, atespace string) (*FocusReport, []byte, error) {
	var focus FocusReport
	raw, err := c.getJSON(ctx, "/v1/costs/focus", atespaceQuery(atespace), &focus)
	if err != nil {
		return nil, nil, err
	}
	return &focus, raw, nil
}

// GetFocusCSV fetches the FOCUS 1.3 split cost allocation rows in CSV format.
func (c *Client) GetFocusCSV(ctx context.Context, atespace string) (string, error) {
	q := atespaceQuery(atespace)
	if q == nil {
		q = url.Values{}
	}
	q.Set("format", "csv")
	body, err := c.get(ctx, "/v1/costs/focus", q, "text/csv")
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func atespaceQuery(atespace string) url.Values {
	if atespace == "" {
		return nil
	}
	return url.Values{"atespace": {atespace}}
}

func (c *Client) get(ctx context.Context, path string, query url.Values, accept string) ([]byte, error) {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", accept)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting %s: %w", u, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned HTTP %d: %s", u, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func (c *Client) getJSON(ctx context.Context, path string, query url.Values, out any) ([]byte, error) {
	body, err := c.get(ctx, path, query, "application/json")
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return nil, fmt.Errorf("decoding %s%s: %w", c.baseURL, path, err)
	}
	return body, nil
}

func latestActorTimestamp(a ActorCost) string {
	if a.LastCall > a.Compute.LastLease {
		return a.LastCall
	}
	return a.Compute.LastLease
}

func preferActorForName(cand, curr ActorCost) bool {
	if (cand.Compute.OpenLeases > 0) != (curr.Compute.OpenLeases > 0) {
		return cand.Compute.OpenLeases > 0
	}
	candLast, currLast := latestActorTimestamp(cand), latestActorTimestamp(curr)
	if candLast != currLast {
		return candLast > currLast
	}
	if cand.Compute.LastLease != curr.Compute.LastLease {
		return cand.Compute.LastLease > curr.Compute.LastLease
	}
	if cand.LastCall != curr.LastCall {
		return cand.LastCall > curr.LastCall
	}
	return cand.UID > curr.UID
}

// CanonicalActorIndex returns the index of the canonical cost record for
// (atespace, name) in actors, or -1 if none matches.
func CanonicalActorIndex(actors []ActorCost, atespace, name string) int {
	if atespace == "" || name == "" {
		return -1
	}
	best := -1
	for i, a := range actors {
		if a.Note != "" || a.Atespace != atespace || a.Name != name {
			continue
		}
		if best == -1 || preferActorForName(a, actors[best]) {
			best = i
		}
	}
	return best
}
