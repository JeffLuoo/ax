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
// and allocation APIs (/v1/costs, /v1/costs/curve, /v1/costs/focus, /v1/leases).
package finops

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

// ComputeSummary summarizes booked and measured compute leases.
type ComputeSummary struct {
	Leases             int64    `json:"leases" yaml:"leases"`
	OpenLeases         int64    `json:"openLeases,omitempty" yaml:"openLeases,omitempty"`
	LeaseSeconds       float64  `json:"leaseSeconds" yaml:"leaseSeconds"`
	VCPUSeconds        float64  `json:"vcpuSeconds" yaml:"vcpuSeconds"`
	GiBSeconds         float64  `json:"gibSeconds" yaml:"gibSeconds"`
	Cost               MoneyMap `json:"cost" yaml:"cost"`
	MeasuredCost       MoneyMap `json:"measuredCost,omitempty" yaml:"measuredCost,omitempty"`
	UnpricedLeases     int64    `json:"unpricedLeases,omitempty" yaml:"unpricedLeases,omitempty"`
	MeasuredCPUSeconds float64  `json:"measuredCpuSeconds,omitempty" yaml:"measuredCpuSeconds,omitempty"`
	LastLease          string   `json:"lastLease,omitempty" yaml:"lastLease,omitempty"`
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

// ActorCost holds full cost attribution (LLM, compute, storage, idle/platform
// overhead, and sub-agent tree rollup) for one actor or Kubernetes pod.
type ActorCost struct {
	Atespace                string           `json:"atespace" yaml:"atespace"`
	Name                    string           `json:"name" yaml:"name"`
	UID                     string           `json:"uid,omitempty" yaml:"uid,omitempty"`
	Tenant                  string           `json:"tenant,omitempty" yaml:"tenant,omitempty"`
	ParentAgentID           string           `json:"parentAgentId,omitempty" yaml:"parentAgentId,omitempty"`
	SubAgents               int              `json:"subAgents,omitempty" yaml:"subAgents,omitempty"`
	Template                string           `json:"template,omitempty" yaml:"template,omitempty"`
	Class                   string           `json:"class,omitempty" yaml:"class,omitempty"`
	IsAgent                 bool             `json:"isAgent" yaml:"isAgent"`
	Calls                   int64            `json:"calls" yaml:"calls"`
	InputTokens             int64            `json:"inputTokens" yaml:"inputTokens"`
	OutputTokens            int64            `json:"outputTokens" yaml:"outputTokens"`
	CacheReadInputTokens    int64            `json:"cacheReadInputTokens,omitempty" yaml:"cacheReadInputTokens,omitempty"`
	CacheWriteInputTokens   int64            `json:"cacheWriteInputTokens,omitempty" yaml:"cacheWriteInputTokens,omitempty"`
	ReasoningOutputTokens   int64            `json:"reasoningOutputTokens,omitempty" yaml:"reasoningOutputTokens,omitempty"`
	Cost                    MoneyMap         `json:"cost" yaml:"cost"`
	UnpricedCalls           int64            `json:"unpricedCalls,omitempty" yaml:"unpricedCalls,omitempty"`
	UsageStatus             map[string]int64 `json:"usageStatus,omitempty" yaml:"usageStatus,omitempty"`
	LastCall                string           `json:"lastCall,omitempty" yaml:"lastCall,omitempty"`
	Compute                 ComputeSummary   `json:"compute" yaml:"compute"`
	TotalCost               MoneyMap         `json:"totalCost" yaml:"totalCost"`
	MeasuredComputeCost     MoneyMap         `json:"measuredComputeCost,omitempty" yaml:"measuredComputeCost,omitempty"`
	IdleShareCost           MoneyMap         `json:"idleShareCost,omitempty" yaml:"idleShareCost,omitempty"`
	StorageCost             MoneyMap         `json:"storageCost,omitempty" yaml:"storageCost,omitempty"`
	EgressCost              MoneyMap         `json:"egressCost,omitempty" yaml:"egressCost,omitempty"`
	PlatformShareCost       MoneyMap         `json:"platformShareCost,omitempty" yaml:"platformShareCost,omitempty"`
	HostingCost             MoneyMap         `json:"hostingCost,omitempty" yaml:"hostingCost,omitempty"`
	MeasuredHostingCost     MoneyMap         `json:"measuredHostingCost,omitempty" yaml:"measuredHostingCost,omitempty"`
	FullyLoadedCost         MoneyMap         `json:"fullyLoadedCost,omitempty" yaml:"fullyLoadedCost,omitempty"`
	MeasuredFullyLoadedCost MoneyMap         `json:"measuredFullyLoadedCost,omitempty" yaml:"measuredFullyLoadedCost,omitempty"`
	TreeLLMCost             MoneyMap         `json:"treeLlmCost,omitempty" yaml:"treeLlmCost,omitempty"`
	TreeComputeCost         MoneyMap         `json:"treeComputeCost,omitempty" yaml:"treeComputeCost,omitempty"`
	TreeTotalCost           MoneyMap         `json:"treeTotalCost,omitempty" yaml:"treeTotalCost,omitempty"`
	TreeFullyLoadedCost     MoneyMap         `json:"treeFullyLoadedCost,omitempty" yaml:"treeFullyLoadedCost,omitempty"`
	Models                  []ModelCost      `json:"models,omitempty" yaml:"models,omitempty"`
}

// VariableHostingUSD returns the variable hosting cost (booked compute + storage + egress).
func (a ActorCost) VariableHostingUSD() float64 {
	return a.Compute.Cost.USD() + a.StorageCost.USD() + a.EgressCost.USD()
}

// MeasuredVariableHostingUSD returns the variable hosting cost using measured CPU usage.
func (a ActorCost) MeasuredVariableHostingUSD() float64 {
	mc := a.MeasuredComputeCost.USD()
	if mc == 0 {
		mc = a.Compute.MeasuredCost.USD()
	}
	return mc + a.StorageCost.USD() + a.EgressCost.USD()
}

// SharedOverheadUSD returns the shared hosting cost (warm-pool idle share + platform overhead share).
func (a ActorCost) SharedOverheadUSD() float64 {
	if h := a.HostingCost.USD(); h > 0 {
		diff := h - a.VariableHostingUSD()
		if diff > 0 {
			return diff
		}
	}
	return a.IdleShareCost.USD() + a.PlatformShareCost.USD()
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
	LLMCost       MoneyMap `json:"llmCost" yaml:"llmCost"`
	ComputeCost   MoneyMap `json:"computeCost" yaml:"computeCost"`
	TotalCost     MoneyMap `json:"totalCost" yaml:"totalCost"`
	LastActivity  string   `json:"lastActivity,omitempty" yaml:"lastActivity,omitempty"`
}

// CurvePoint represents a single active-hours point on the hosting cost curve.
type CurvePoint struct {
	ActiveHours float64 `json:"activeHours" yaml:"activeHours"`
	Variable    float64 `json:"variable" yaml:"variable"`
	IdleShare   float64 `json:"idleShare" yaml:"idleShare"`
	Storage     float64 `json:"storage" yaml:"storage"`
	Platform    float64 `json:"platform" yaml:"platform"`
	FullyLoaded float64 `json:"fullyLoaded" yaml:"fullyLoaded"`
	UnderTarget bool    `json:"underTarget" yaml:"underTarget"`
}

// TierCurve holds the hosting cost sensitivity curve for one pricing tier.
type TierCurve struct {
	Tier                string       `json:"tier" yaml:"tier"`
	PerActiveHour       float64      `json:"perActiveHour" yaml:"perActiveHour"`
	LoadedPerActiveHour float64      `json:"loadedPerActiveHour" yaml:"loadedPerActiveHour"`
	Fixed               float64      `json:"fixed" yaml:"fixed"`
	BreakEvenHours      float64      `json:"breakEvenHours" yaml:"breakEvenHours"`
	AlwaysOn            float64      `json:"alwaysOn" yaml:"alwaysOn"`
	Points              []CurvePoint `json:"points" yaml:"points"`
}

// CurveReport is the response from GET /v1/costs/curve (and embedded in /v1/costs).
type CurveReport struct {
	TargetUSD           float64     `json:"targetUsd" yaml:"targetUsd"`
	PoolUtilization     float64     `json:"poolUtilization" yaml:"poolUtilization"`
	StoragePerAgentUSD  float64     `json:"storagePerAgentUsd" yaml:"storagePerAgentUsd"`
	PlatformPerAgentUSD float64     `json:"platformPerAgentUsd" yaml:"platformPerAgentUsd"`
	Tiers               []TierCurve `json:"tiers" yaml:"tiers"`
}

// FocusElement describes one resource element in FOCUS AllocatedMethodDetails.
type FocusElement struct {
	AllocatedRatio           float64 `json:"AllocatedRatio,omitempty" yaml:"AllocatedRatio,omitempty"`
	UsageQuantity            float64 `json:"UsageQuantity,omitempty" yaml:"UsageQuantity,omitempty"`
	UsageUnit                string  `json:"UsageUnit,omitempty" yaml:"UsageUnit,omitempty"`
	PricePerUnit             float64 `json:"x_PricePerUnit,omitempty" yaml:"x_PricePerUnit,omitempty"`
	UnusedCapacityResourceID string  `json:"x_UnusedCapacityResourceId,omitempty" yaml:"x_UnusedCapacityResourceId,omitempty"`
}

// FocusMethodDetails holds FOCUS 1.3 AllocatedMethodDetails.
type FocusMethodDetails struct {
	Elements []FocusElement `json:"Elements,omitempty" yaml:"Elements,omitempty"`
}

// FocusCharge represents one FOCUS 1.3 split cost allocation row.
type FocusCharge struct {
	OriginChargeID         string             `json:"OriginChargeId" yaml:"OriginChargeId"`
	BillingCurrency        string             `json:"BillingCurrency" yaml:"BillingCurrency"`
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

// FocusReconciliation summarizes bottom-up vs. origin charge reconciliation.
type FocusReconciliation struct {
	ResourceID    string   `json:"resourceId" yaml:"resourceId"`
	Pool          string   `json:"pool" yaml:"pool"`
	PeriodStart   string   `json:"periodStart" yaml:"periodStart"`
	PeriodEnd     string   `json:"periodEnd" yaml:"periodEnd"`
	Estimated     bool     `json:"estimated" yaml:"estimated"`
	OriginCost    float64  `json:"originCost" yaml:"originCost"`
	Allocated     float64  `json:"allocated" yaml:"allocated"`
	Unallocated   float64  `json:"unallocated" yaml:"unallocated"`
	Agents        int      `json:"agents" yaml:"agents"`
	UnsizedLeases int      `json:"unsizedLeases" yaml:"unsizedLeases"`
	Problems      []string `json:"problems,omitempty" yaml:"problems,omitempty"`
}

// FocusReport is the response from GET /v1/costs/focus.
type FocusReport struct {
	OriginCharge   FocusCharge          `json:"originCharge" yaml:"originCharge"`
	Charges        []FocusCharge        `json:"charges" yaml:"charges"`
	Reconciliation *FocusReconciliation `json:"reconciliation,omitempty" yaml:"reconciliation,omitempty"`
}

// CostReport is the response from GET /v1/costs.
type CostReport struct {
	Since               string               `json:"since" yaml:"since"`
	GeneratedAt         string               `json:"generatedAt" yaml:"generatedAt"`
	Totals              Totals               `json:"totals" yaml:"totals"`
	Compute             ComputeSummary       `json:"compute" yaml:"compute"`
	TotalCost           MoneyMap             `json:"totalCost" yaml:"totalCost"`
	FullyLoadedCost     MoneyMap             `json:"fullyLoadedCost" yaml:"fullyLoadedCost"`
	Actors              []ActorCost          `json:"actors" yaml:"actors"`
	Templates           []TemplateCost       `json:"templates,omitempty" yaml:"templates,omitempty"`
	Sessions            []SessionCost        `json:"sessions,omitempty" yaml:"sessions,omitempty"`
	HostingCurve        *CurveReport         `json:"hostingCurve,omitempty" yaml:"hostingCurve,omitempty"`
	FocusCharges        []FocusCharge        `json:"focusCharges,omitempty" yaml:"focusCharges,omitempty"`
	FocusReconciliation *FocusReconciliation `json:"focusReconciliation,omitempty" yaml:"focusReconciliation,omitempty"`
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

// GetCosts fetches the full cost summary from GET /v1/costs.
func (c *Client) GetCosts(ctx context.Context) (*CostReport, error) {
	var report CostReport
	if err := c.getJSON(ctx, "/v1/costs", &report); err != nil {
		return nil, err
	}
	return &report, nil
}

// GetActorCost looks up a single actor/task by (atespace, name), "<atespace>/<name>",
// or Actor UID (`gen_ai.agent.id`), and returns the matching ActorCost, its
// sessions, its direct child sub-agents, and the full CostReport.
func (c *Client) GetActorCost(ctx context.Context, atespace, nameOrUID string) (*ActorCost, []SessionCost, []ActorCost, *CostReport, error) {
	report, err := c.GetCosts(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	targetAtespace := atespace
	targetName := strings.TrimSpace(nameOrUID)
	if parts := strings.SplitN(targetName, "/", 2); len(parts) == 2 {
		targetAtespace = parts[0]
		targetName = parts[1]
	}

	var matched *ActorCost
	for i := range report.Actors {
		a := &report.Actors[i]
		if a.Name == targetName && (targetAtespace == "" || a.Atespace == targetAtespace) {
			matched = a
			break
		}
	}
	if matched == nil {
		// Fall back to UID prefix or exact name match across atespaces.
		for i := range report.Actors {
			a := &report.Actors[i]
			if (a.UID != "" && (a.UID == targetName || strings.HasPrefix(a.UID, targetName))) || a.Name == targetName {
				matched = a
				break
			}
		}
	}
	if matched == nil {
		scope := targetName
		if targetAtespace != "" {
			scope = targetAtespace + "/" + targetName
		}
		return nil, nil, nil, report, fmt.Errorf("no FinOps cost record found for %q", scope)
	}

	fullActorKey := matched.Atespace + "/" + matched.Name
	var sessions []SessionCost
	for _, s := range report.Sessions {
		if s.Actor == fullActorKey || s.Actor == matched.Name || (matched.UID != "" && s.ActorUID == matched.UID) {
			sessions = append(sessions, s)
		}
	}

	var children []ActorCost
	if matched.UID != "" {
		for _, a := range report.Actors {
			if a.ParentAgentID == matched.UID {
				children = append(children, a)
			}
		}
	}

	return matched, sessions, children, report, nil
}

// GetCurve fetches the < $1/agent-month hosting cost sensitivity curve from GET /v1/costs/curve.
func (c *Client) GetCurve(ctx context.Context) (*CurveReport, error) {
	var curve CurveReport
	if err := c.getJSON(ctx, "/v1/costs/curve", &curve); err != nil {
		return nil, err
	}
	return &curve, nil
}

// GetFocus fetches the FOCUS 1.3 split cost allocation report from GET /v1/costs/focus.
func (c *Client) GetFocus(ctx context.Context) (*FocusReport, error) {
	var focus FocusReport
	if err := c.getJSON(ctx, "/v1/costs/focus", &focus); err != nil {
		return nil, err
	}
	return &focus, nil
}

// GetFocusCSV fetches the FOCUS 1.3 split cost allocation rows in CSV format.
func (c *Client) GetFocusCSV(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/costs/focus?format=csv", nil)
	if err != nil {
		return "", err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("requesting %s/v1/costs/focus?format=csv: %w", c.baseURL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s/v1/costs/focus?format=csv returned HTTP %d: %s", c.baseURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return string(body), nil
}

func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("requesting %s%s: %w", c.baseURL, path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s%s returned HTTP %d: %s", c.baseURL, path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decoding %s%s: %w", c.baseURL, path, err)
	}
	return nil
}
