// Package clawpro is the official Go client for the ClawPro Instagram outbound
// API — connect sending accounts, run campaigns, score leads, manage webhooks,
// and read usage.
//
//	c := clawpro.New(os.Getenv("CLAWPRO_API_KEY"))
//	accounts, err := c.ListAccounts(context.Background())
package clawpro

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultBaseURL = "https://api.tryclawpro.com"

// ─────────────────────────────── Types ──────────────────────────────────────

type AccountStatus string
type ProxyType string

type Account struct {
	ID         string        `json:"id"`
	Username   string        `json:"username"`
	Status     AccountStatus `json:"status"`
	WarmupTier int           `json:"warmupTier"`
	DailyDMCap int           `json:"dailyDmCap"`
}

type CampaignStatus string

type CampaignTarget struct {
	Username          string  `json:"username"`
	LastCheckedPostID *string `json:"lastCheckedPostId"`
}

type Campaign struct {
	ID            string           `json:"id"`
	AccountID     string           `json:"accountId"`
	Name          string           `json:"name"`
	Status        CampaignStatus   `json:"status"`
	Targets       []CampaignTarget `json:"targets"`
	Offer         string           `json:"offer"`
	ICPCriteria   string           `json:"icpCriteria"`
	DailyDMTarget int              `json:"dailyDmTarget"`
}

type LeadStatus string

type Message struct {
	ID        string  `json:"id"`
	Direction string  `json:"direction"`
	Body      string  `json:"body"`
	Status    string  `json:"status"`
	SentAt    *string `json:"sentAt"`
	CreatedAt string  `json:"createdAt"`
}

type Lead struct {
	ID                   string     `json:"id"`
	IGUsername           string     `json:"igUsername"`
	FullName             *string    `json:"fullName"`
	SourceTargetUsername string     `json:"sourceTargetUsername"`
	EngagementType       string     `json:"engagementType"`
	CommentText          *string    `json:"commentText"`
	Score                *int       `json:"score"`
	ScoreReason          *string    `json:"scoreReason"`
	Status               LeadStatus `json:"status"`
	Messages             []Message  `json:"messages,omitempty"`
}

type Webhook struct {
	ID           string   `json:"id"`
	Label        string   `json:"label"`
	URL          string   `json:"url"`
	Type         string   `json:"type"`
	Events       []string `json:"events"`
	Active       bool     `json:"active"`
	LastStatus   *int     `json:"lastStatus"`
	LastDelivery *string  `json:"lastDeliveryAt"`
	LastError    *string  `json:"lastError"`
	Secret       string   `json:"secret,omitempty"` // only on create
}

type WebhookDelivery struct {
	ID             string  `json:"id"`
	Event          string  `json:"event"`
	Status         string  `json:"status"`
	Attempts       int     `json:"attempts"`
	LastStatusCode *int    `json:"lastStatusCode"`
	LastError      *string `json:"lastError"`
	NextRetryAt    *string `json:"nextRetryAt"`
	DeliveredAt    *string `json:"deliveredAt"`
	CreatedAt      string  `json:"createdAt"`
}

type APIKey struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Environment string  `json:"environment"`
	Access      string  `json:"access"`
	KeyPrefix   string  `json:"keyPrefix"`
	CreatedAt   string  `json:"createdAt"`
	ExpiresAt   *string `json:"expiresAt"`
	LastUsedAt  *string `json:"lastUsedAt"`
}

type UsageSummary struct {
	CallsThisMonth int `json:"callsThisMonth"`
	Calls24h       int `json:"calls24h"`
	Errors         int `json:"errors"`
	AvgLatencyMs   int `json:"avgLatencyMs"`
}

type RequestLog struct {
	ID        string  `json:"id"`
	RequestID string  `json:"requestId"`
	Method    string  `json:"method"`
	Path      string  `json:"path"`
	Status    int     `json:"status"`
	LatencyMs int     `json:"latencyMs"`
	Error     *string `json:"error"`
	CreatedAt string  `json:"createdAt"`
	KeyName   *string `json:"keyName"`
	KeyPrefix *string `json:"keyPrefix"`
}

// Error is returned for any non-2xx API response.
type Error struct {
	Status    int
	Code      string
	Message   string
	RequestID string
}

func (e *Error) Error() string {
	if e.RequestID != "" {
		return fmt.Sprintf("clawpro: %d %s (request %s)", e.Status, e.Message, e.RequestID)
	}
	return fmt.Sprintf("clawpro: %d %s", e.Status, e.Message)
}

// ─────────────────────────────── Client ─────────────────────────────────────

// Client is a ClawPro API client. Create one with New.
type Client struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
	maxRetries int
}

// Option configures a Client.
type Option func(*Client)

// WithBaseURL overrides the API origin (e.g. staging / self-hosted).
func WithBaseURL(u string) Option { return func(c *Client) { c.baseURL = strings.TrimRight(u, "/") } }

// WithHTTPClient supplies a custom *http.Client.
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.httpClient = h } }

// WithMaxRetries sets the number of retries for transient failures (default 2).
func WithMaxRetries(n int) Option { return func(c *Client) { c.maxRetries = n } }

// New creates a client. apiKey is a sk_live_… / sk_test_… developer key.
func New(apiKey string, opts ...Option) *Client {
	c := &Client{
		apiKey:     apiKey,
		baseURL:    defaultBaseURL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		maxRetries: 2,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

type errBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func backoff(attempt int) time.Duration {
	base := 250 * math.Pow(2, float64(attempt))
	jitter := 0.8 + rand.Float64()*0.4
	return time.Duration(base*jitter) * time.Millisecond
}

func (c *Client) do(ctx context.Context, method, path string, body any, query url.Values, out any) error {
	endpoint := c.baseURL + "/api/instagram" + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	idempotent := method == http.MethodGet || method == http.MethodDelete || method == http.MethodPut

	for attempt := 0; ; attempt++ {
		var reader io.Reader
		if raw != nil {
			reader = bytes.NewReader(raw)
		}
		req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
		if err != nil {
			return err
		}
		req.Header.Set("X-API-Key", c.apiKey)
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			if idempotent && attempt < c.maxRetries {
				time.Sleep(backoff(attempt))
				continue
			}
			return &Error{Status: 0, Code: "network_error", Message: err.Error()}
		}

		transient := resp.StatusCode == 429 || resp.StatusCode >= 500
		if transient && (resp.StatusCode == 429 || idempotent) && attempt < c.maxRetries {
			wait := backoff(attempt)
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if s, e := strconv.Atoi(ra); e == nil && s > 0 {
					wait = time.Duration(s) * time.Second
				}
			}
			resp.Body.Close()
			time.Sleep(wait)
			continue
		}

		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			var eb errBody
			_ = json.Unmarshal(data, &eb)
			msg := eb.Message
			if msg == "" {
				msg = eb.Error
			}
			if msg == "" {
				msg = resp.Status
			}
			return &Error{Status: resp.StatusCode, Code: eb.Error, Message: msg, RequestID: resp.Header.Get("X-Request-Id")}
		}
		if out != nil {
			return json.Unmarshal(data, out)
		}
		return nil
	}
}

// ───────────────────────────── Accounts ─────────────────────────────────────

func (c *Client) ListAccounts(ctx context.Context) ([]Account, error) {
	var r struct {
		Accounts []Account `json:"accounts"`
	}
	return r.Accounts, c.do(ctx, http.MethodGet, "/accounts", nil, nil, &r)
}

type CreateAccountParams struct {
	Username  string `json:"username"`
	Country   string `json:"country,omitempty"`
	ProxyType string `json:"proxyType,omitempty"`
	ProxyURL  string `json:"proxyUrl,omitempty"`
	Timezone  string `json:"timezone,omitempty"`
}

func (c *Client) CreateAccount(ctx context.Context, p CreateAccountParams) (*Account, error) {
	var r struct {
		Account Account `json:"account"`
	}
	if err := c.do(ctx, http.MethodPost, "/accounts", p, nil, &r); err != nil {
		return nil, err
	}
	return &r.Account, nil
}

func (c *Client) DeleteAccount(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/accounts/"+id, nil, nil, nil)
}

// ───────────────────────────── Campaigns ────────────────────────────────────

func (c *Client) ListCampaigns(ctx context.Context) ([]Campaign, error) {
	var r struct {
		Campaigns []Campaign `json:"campaigns"`
	}
	return r.Campaigns, c.do(ctx, http.MethodGet, "/campaigns", nil, nil, &r)
}

type CreateCampaignParams struct {
	AccountID     string   `json:"accountId"`
	Name          string   `json:"name"`
	Targets       []string `json:"targets"`
	Offer         string   `json:"offer,omitempty"`
	ICPCriteria   string   `json:"icpCriteria,omitempty"`
	DailyDMTarget int      `json:"dailyDmTarget,omitempty"`
}

func (c *Client) CreateCampaign(ctx context.Context, p CreateCampaignParams) (*Campaign, error) {
	var r struct {
		Campaign Campaign `json:"campaign"`
	}
	if err := c.do(ctx, http.MethodPost, "/campaigns", p, nil, &r); err != nil {
		return nil, err
	}
	return &r.Campaign, nil
}

type RunResult struct {
	Discovered int `json:"discovered"`
	Scored     int `json:"scored"`
	Queued     int `json:"queued"`
}

func (c *Client) RunCampaign(ctx context.Context, id string) (*RunResult, error) {
	var r RunResult
	return &r, c.do(ctx, http.MethodPost, "/campaigns/"+id+"/run", nil, nil, &r)
}

func (c *Client) ListLeads(ctx context.Context, campaignID string, status string) ([]Lead, error) {
	q := url.Values{}
	if status != "" {
		q.Set("status", status)
	}
	var r struct {
		Leads []Lead `json:"leads"`
	}
	return r.Leads, c.do(ctx, http.MethodGet, "/campaigns/"+campaignID+"/leads", nil, q, &r)
}

func (c *Client) Inbox(ctx context.Context, campaignID string) ([]Lead, error) {
	var r struct {
		Threads []Lead `json:"threads"`
	}
	return r.Threads, c.do(ctx, http.MethodGet, "/campaigns/"+campaignID+"/inbox", nil, nil, &r)
}

// ─────────────────────────────── Leads ──────────────────────────────────────

// UpdateLead advances a lead, e.g. status "booked" (fires the lead.booked webhook).
func (c *Client) UpdateLead(ctx context.Context, id, status string) (*Lead, error) {
	var r struct {
		Lead Lead `json:"lead"`
	}
	if err := c.do(ctx, http.MethodPatch, "/leads/"+id, map[string]string{"status": status}, nil, &r); err != nil {
		return nil, err
	}
	return &r.Lead, nil
}

// IngestLead is a lead pushed in from your own scraper — who to DM + the
// engagement context that feeds the DM.
type IngestLead struct {
	Username       string  `json:"username"`
	EngagedWith    string  `json:"engagedWith,omitempty"`
	EngagementType string  `json:"engagementType,omitempty"`
	Comment        *string `json:"comment,omitempty"`
	FullName       *string `json:"fullName,omitempty"`
}

// AddLeadsResult summarizes a bulk lead push.
type AddLeadsResult struct {
	Created int    `json:"created"`
	Skipped int    `json:"skipped"`
	Total   int    `json:"total"`
	Status  string `json:"status"`
}

// AddLeads pushes externally-sourced leads into a campaign in bulk
// (bring-your-own-scraper). Deduped on username; up to 500. score routes them
// through the ICP scorer first.
func (c *Client) AddLeads(ctx context.Context, campaignID string, leads []IngestLead, score bool) (*AddLeadsResult, error) {
	q := url.Values{}
	if score {
		q.Set("score", "1")
	}
	var r AddLeadsResult
	return &r, c.do(ctx, http.MethodPost, "/campaigns/"+campaignID+"/leads", leads, q, &r)
}

// ReplyToLead sends a reply / follow-up to a lead in its existing thread
// (inbox-write — manage the conversation via API).
func (c *Client) ReplyToLead(ctx context.Context, leadID, text string) error {
	return c.do(ctx, http.MethodPost, "/leads/"+leadID+"/reply", map[string]string{"text": text}, nil, nil)
}

// ────────────────────────────── Webhooks ────────────────────────────────────

func (c *Client) ListWebhooks(ctx context.Context) ([]Webhook, error) {
	var r struct {
		Webhooks []Webhook `json:"webhooks"`
	}
	return r.Webhooks, c.do(ctx, http.MethodGet, "/webhooks", nil, nil, &r)
}

func (c *Client) WebhookEvents(ctx context.Context) ([]string, error) {
	var r struct {
		Events []string `json:"events"`
	}
	return r.Events, c.do(ctx, http.MethodGet, "/webhooks/events", nil, nil, &r)
}

type CreateWebhookParams struct {
	URL    string   `json:"url"`
	Type   string   `json:"type,omitempty"`
	Events []string `json:"events,omitempty"`
	Label  string   `json:"label,omitempty"`
}

func (c *Client) CreateWebhook(ctx context.Context, p CreateWebhookParams) (*Webhook, error) {
	if p.Type == "" {
		p.Type = "generic"
	}
	var r struct {
		Webhook Webhook `json:"webhook"`
	}
	if err := c.do(ctx, http.MethodPost, "/webhooks", p, nil, &r); err != nil {
		return nil, err
	}
	return &r.Webhook, nil
}

func (c *Client) DeleteWebhook(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/webhooks/"+id, nil, nil, nil)
}

// ───────────────────────────── API keys ─────────────────────────────────────

func (c *Client) ListKeys(ctx context.Context) ([]APIKey, error) {
	var r struct {
		Keys []APIKey `json:"keys"`
	}
	return r.Keys, c.do(ctx, http.MethodGet, "/keys", nil, nil, &r)
}

type CreateKeyParams struct {
	Name        string `json:"name"`
	Environment string `json:"environment,omitempty"`
	Access      string `json:"access,omitempty"`
	ExpiresAt   string `json:"expiresAt,omitempty"`
}

// CreateKey returns the one-time plaintext Secret plus the stored Key record.
func (c *Client) CreateKey(ctx context.Context, p CreateKeyParams) (secret string, key *APIKey, err error) {
	var r struct {
		Secret string `json:"secret"`
		Key    APIKey `json:"key"`
	}
	if err = c.do(ctx, http.MethodPost, "/keys", p, nil, &r); err != nil {
		return "", nil, err
	}
	return r.Secret, &r.Key, nil
}

func (c *Client) RevokeKey(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/keys/"+id, nil, nil, nil)
}

// ─────────────────────────────── Usage ──────────────────────────────────────

func (c *Client) Usage(ctx context.Context) (*UsageSummary, error) {
	var r UsageSummary
	return &r, c.do(ctx, http.MethodGet, "/usage", nil, nil, &r)
}

// LogFilter narrows the request log. Zero values are omitted.
type LogFilter struct {
	Key       string
	Method    string
	Status    string // "2xx" | "4xx" | "5xx"
	From      string
	To        string
	Endpoint  string
	RequestID string
	Search    string
	Limit     int
	Offset    int
}

func (f LogFilter) values() url.Values {
	q := url.Values{}
	set := func(k, v string) {
		if v != "" {
			q.Set(k, v)
		}
	}
	set("key", f.Key)
	set("method", f.Method)
	set("status", f.Status)
	set("from", f.From)
	set("to", f.To)
	set("endpoint", f.Endpoint)
	set("requestId", f.RequestID)
	set("search", f.Search)
	if f.Limit > 0 {
		q.Set("limit", strconv.Itoa(f.Limit))
	}
	if f.Offset > 0 {
		q.Set("offset", strconv.Itoa(f.Offset))
	}
	return q
}

// Logs returns one page of developer-API request logs.
func (c *Client) Logs(ctx context.Context, f LogFilter) ([]RequestLog, error) {
	var r struct {
		Logs []RequestLog `json:"logs"`
	}
	return r.Logs, c.do(ctx, http.MethodGet, "/logs", nil, f.values(), &r)
}

// ─────────────────────────── Webhook verification ────────────────────────────

// VerifyWebhook verifies a ClawPro webhook's HMAC signature (the
// X-Souk-Signature header). payload is the raw request body; tolerance rejects
// signatures older than the given duration (0 disables). Returns true if valid.
func VerifyWebhook(payload []byte, signature, secret string, tolerance time.Duration) bool {
	var t, v1 string
	for _, p := range strings.Split(signature, ",") {
		kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "t":
			t = kv[1]
		case "v1":
			v1 = kv[1]
		}
	}
	if t == "" || v1 == "" {
		return false
	}
	ts, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return false
	}
	if tolerance > 0 {
		d := time.Since(time.Unix(ts, 0))
		if d < 0 {
			d = -d
		}
		if d > tolerance {
			return false
		}
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(t + "." + string(payload)))
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(v1))
}
