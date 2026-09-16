package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

type config struct {
	DatabaseURL string

	StatusURL string
	TokenURL  string
	ClientID  string
	APIToken  string
	APISecret string

	Interval    time.Duration
	RunTimeout  time.Duration
	HTTPTimeout time.Duration
	BatchSize   int
	MaxAttempts int
}

func loadConfig() (*config, error) {
	cfg := &config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		StatusURL:   envOr("BOOMPAY_STATUS_URL", "https://b2b-api.boompay.in/api/v1/prod/payout/w/ur/initiated-payout-status"),
		TokenURL:    os.Getenv("BOOMPAY_TOKEN_URL"),
		ClientID:    os.Getenv("BOOMPAY_CLIENT_ID"),
		APIToken:    os.Getenv("BOOMPAY_API_TOKEN"),
		APISecret:   os.Getenv("BOOMPAY_API_SECRET"),
		Interval:    envDuration("POLL_INTERVAL", 5*time.Minute),
		RunTimeout:  envDuration("RUN_TIMEOUT", 4*time.Minute),
		HTTPTimeout: envDuration("HTTP_TIMEOUT", 30*time.Second),
		BatchSize:   envInt("BATCH_SIZE", 500),
		MaxAttempts: envInt("HTTP_MAX_ATTEMPTS", 3),
	}

	// Fail loudly at startup instead of silently POSTing to an empty URL.
	missing := make([]string, 0, 5)
	for name, value := range map[string]string{
		"DATABASE_URL":        cfg.DatabaseURL,
		"BOOMPAY_TOKEN_URL":   cfg.TokenURL,
		"BOOMPAY_CLIENT_ID":   cfg.ClientID,
		"BOOMPAY_API_TOKEN":   cfg.APIToken,
		"BOOMPAY_API_SECRET":  cfg.APISecret,
	} {
		if strings.TrimSpace(value) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	// The run must finish before the next tick fires, otherwise runs overlap.
	if cfg.RunTimeout >= cfg.Interval {
		cfg.RunTimeout = cfg.Interval - (cfg.Interval / 10)
	}

	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v, err := time.ParseDuration(envOr(key, "")); err == nil && v > 0 {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v, err := strconv.Atoi(envOr(key, "")); err == nil && v > 0 {
		return v
	}
	return fallback
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

const (
	statusPending = "PENDING"
	statusSuccess = "SUCCESS"
	statusFailed  = "FAILED"

	providerBoom = "BOOM"
)

type Payout struct {
	PayoutTransactionID string
	RequestID           string
}

type PayoutStatusResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Status  string `json:"status"`
	Data    struct {
		StatusCode int    `json:"statusCode"`
		ReqStatus  string `json:"reqStatus"`
		UTR        string `json:"utr"`
	} `json:"data"`
}

type accessTokenResponse struct {
	Success     bool   `json:"success"`
	Message     string `json:"message"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	AccessToken string `json:"access_token"`
}

// mapStatus translates a provider status into an internal one.
//
// NOTE: adjust these cases to the exact vocabulary BoomPay returns and to the
// values your payout_transaction_status column accepts. Anything unrecognised
// is deliberately left alone rather than written to the database.
func mapStatus(reqStatus string) (string, bool) {
	switch strings.ToUpper(strings.TrimSpace(reqStatus)) {
	case "SUCCESS", "SUCCESSFUL", "COMPLETED", "PAID", "SETTLED":
		return statusSuccess, true
	case "FAILED", "FAILURE", "REJECTED", "REVERSED", "RETURNED", "CANCELLED":
		return statusFailed, true
	case "PENDING", "INITIATED", "PROCESSING", "IN_PROCESS", "QUEUED":
		return statusPending, true
	default:
		return "", false
	}
}

// ---------------------------------------------------------------------------
// HTTP plumbing
// ---------------------------------------------------------------------------

// apiError carries the HTTP status so callers can react to 401s specifically.
type apiError struct {
	StatusCode int
	Body       string
}

func (e *apiError) Error() string {
	body := e.Body
	if len(body) > 300 {
		body = body[:300] + "..."
	}
	return fmt.Sprintf("http %d: %s", e.StatusCode, body)
}

const maxResponseBytes = 1 << 20 // 1 MiB

func postJSON(
	ctx context.Context,
	client *http.Client,
	attempts int,
	url string,
	headers map[string]string,
	body any,
	out any,
) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	var lastErr error

	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			backoff := time.Duration(1<<uint(attempt-2)) * time.Second
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}

		err := doPost(ctx, client, url, headers, payload, out)
		if err == nil {
			return nil
		}
		lastErr = err

		// Don't burn attempts on failures a retry cannot fix.
		if ctx.Err() != nil {
			return err
		}
		var apiErr *apiError
		if errors.As(err, &apiErr) && apiErr.StatusCode < 500 && apiErr.StatusCode != http.StatusTooManyRequests {
			return err
		}
	}

	return fmt.Errorf("after %d attempts: %w", attempts, lastErr)
}

func doPost(
	ctx context.Context,
	client *http.Client,
	url string,
	headers map[string]string,
	payload []byte,
	out any,
) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer func() {
		// Drain so the connection can be reused by keep-alive.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
		_ = resp.Body.Close()
	}()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return fmt.Errorf("read body (status %d): %w", resp.StatusCode, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &apiError{StatusCode: resp.StatusCode, Body: string(raw)}
	}

	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode body (status %d): %w", resp.StatusCode, err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Access token cache
// ---------------------------------------------------------------------------

// tokenExpiryMargin keeps a token from being used in the last moments of its
// life, where it could expire between the check and the server-side validation.
const tokenExpiryMargin = 60 * time.Second

type tokenManager struct {
	cfg    *config
	client *http.Client

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

func newTokenManager(cfg *config, client *http.Client) *tokenManager {
	return &tokenManager{cfg: cfg, client: client}
}

// Token returns a cached token, fetching a new one when it is missing, close to
// expiry, or when forceRefresh is set (used after a 401).
func (tm *tokenManager) Token(ctx context.Context, forceRefresh bool) (string, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if !forceRefresh && tm.token != "" && time.Now().Before(tm.expiresAt) {
		return tm.token, nil
	}

	var res accessTokenResponse
	err := postJSON(ctx, tm.client, tm.cfg.MaxAttempts, tm.cfg.TokenURL, map[string]string{
		"X-CLIENT-ID":  tm.cfg.ClientID,
		"X-API-TOKEN":  tm.cfg.APIToken,
		"X-API-SECRET": tm.cfg.APISecret,
	}, map[string]any{}, &res)
	if err != nil {
		return "", fmt.Errorf("fetch access token: %w", err)
	}

	if !res.Success || res.AccessToken == "" {
		return "", fmt.Errorf("access token rejected: %s", messageOr(res.Message, "no message returned"))
	}

	ttl := time.Duration(res.ExpiresIn) * time.Second
	if ttl <= tokenExpiryMargin {
		ttl = 5 * time.Minute // provider sent nothing usable; re-fetch soon
	}

	tm.token = res.AccessToken
	tm.expiresAt = time.Now().Add(ttl - tokenExpiryMargin)

	return tm.token, nil
}

func messageOr(msg, fallback string) string {
	if strings.TrimSpace(msg) == "" {
		return fallback
	}
	return msg
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

type worker struct {
	cfg    *config
	db     *sql.DB
	client *http.Client
	tokens *tokenManager
}

func (w *worker) runOnce(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, w.cfg.RunTimeout)
	defer cancel()

	start := time.Now()
	log.Println("payout status cron started")

	payouts, err := w.pendingPayouts(ctx)
	if err != nil {
		log.Printf("failed to load pending payouts: %v", err)
		return
	}

	if len(payouts) == 0 {
		log.Println("no pending payouts found")
		return
	}

	log.Printf("found %d pending payouts to check", len(payouts))

	var settled, stillPending, failedChecks int

	for _, payout := range payouts {
		if ctx.Err() != nil {
			log.Printf("run cancelled after %d/%d payouts: %v", settled+stillPending+failedChecks, len(payouts), ctx.Err())
			break
		}

		status, utr, err := w.checkStatus(ctx, payout.RequestID)
		if err != nil {
			failedChecks++
			log.Printf("status check failed requestID=%s: %v", payout.RequestID, err)
			continue
		}

		// Nothing has changed yet. Writing here would bump updated_at and, when
		// the UTR is still empty, clobber operator_transaction_id.
		if status == statusPending {
			stillPending++
			continue
		}

		updated, err := w.updatePayoutStatus(ctx, payout.PayoutTransactionID, status, utr)
		if err != nil {
			failedChecks++
			log.Printf("failed to update payoutTransactionID=%s requestID=%s: %v",
				payout.PayoutTransactionID, payout.RequestID, err)
			continue
		}

		if !updated {
			// Someone else moved it out of PENDING while we were checking.
			log.Printf("skipped payoutTransactionID=%s requestID=%s: no longer pending",
				payout.PayoutTransactionID, payout.RequestID)
			continue
		}

		settled++
		log.Printf("updated payoutTransactionID=%s requestID=%s status=%s utr=%q",
			payout.PayoutTransactionID, payout.RequestID, status, utr)
	}

	log.Printf("payout status cron completed in %s (settled=%d pending=%d errors=%d)",
		time.Since(start).Round(time.Millisecond), settled, stillPending, failedChecks)
}

func (w *worker) pendingPayouts(ctx context.Context) ([]Payout, error) {
	const query = `
		SELECT payout_transaction_id,
		       COALESCE(partner_request_id, '')
		FROM payout_transactions
		WHERE payout_transaction_status = $1
		  AND api_provider = $2
		ORDER BY updated_at ASC
		LIMIT $3;
	`

	rows, err := w.db.QueryContext(ctx, query, statusPending, providerBoom, w.cfg.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("query pending payouts: %w", err)
	}
	defer rows.Close()

	payouts := make([]Payout, 0, w.cfg.BatchSize)

	for rows.Next() {
		var p Payout
		if err := rows.Scan(&p.PayoutTransactionID, &p.RequestID); err != nil {
			return nil, fmt.Errorf("scan pending payout: %w", err)
		}
		if p.RequestID == "" {
			log.Printf("skipping payoutTransactionID=%s: no partner_request_id", p.PayoutTransactionID)
			continue
		}
		payouts = append(payouts, p)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending payouts: %w", err)
	}

	return payouts, nil
}

// updatePayoutStatus reports whether a row was actually transitioned. The
// PENDING guard in the WHERE clause makes this safe against a concurrent
// writer, and COALESCE/NULLIF keeps an existing UTR when the response has none.
func (w *worker) updatePayoutStatus(ctx context.Context, payoutTransactionID, status, utr string) (bool, error) {
	const query = `
		UPDATE payout_transactions
		SET payout_transaction_status = $1,
		    operator_transaction_id   = COALESCE(NULLIF($2, ''), operator_transaction_id),
		    updated_at                = NOW()
		WHERE payout_transaction_id     = $3
		  AND api_provider              = $4
		  AND payout_transaction_status = $5;
	`

	res, err := w.db.ExecContext(ctx, query, status, utr, payoutTransactionID, providerBoom, statusPending)
	if err != nil {
		return false, fmt.Errorf("update payout: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected: %w", err)
	}

	return rows > 0, nil
}

func (w *worker) checkStatus(ctx context.Context, requestID string) (string, string, error) {
	res, err := w.requestStatus(ctx, requestID, false)
	if err != nil {
		// An expired token mid-run is expected; refresh once and retry.
		var apiErr *apiError
		if errors.As(err, &apiErr) && (apiErr.StatusCode == http.StatusUnauthorized || apiErr.StatusCode == http.StatusForbidden) {
			res, err = w.requestStatus(ctx, requestID, true)
		}
		if err != nil {
			return "", "", err
		}
	}

	if !res.Success {
		return "", "", fmt.Errorf("provider rejected status check: %s", messageOr(res.Message, "no message returned"))
	}

	status, ok := mapStatus(res.Data.ReqStatus)
	if !ok {
		return "", "", fmt.Errorf("unrecognised provider status %q (statusCode=%d)", res.Data.ReqStatus, res.Data.StatusCode)
	}

	return status, strings.TrimSpace(res.Data.UTR), nil
}

func (w *worker) requestStatus(ctx context.Context, requestID string, forceRefresh bool) (*PayoutStatusResponse, error) {
	token, err := w.tokens.Token(ctx, forceRefresh)
	if err != nil {
		return nil, err
	}

	var res PayoutStatusResponse
	err = postJSON(ctx, w.client, w.cfg.MaxAttempts, w.cfg.StatusURL, map[string]string{
		"Authorization": "Bearer " + token,
	}, map[string]any{"requestID": requestID}, &res)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

// ---------------------------------------------------------------------------
// Entrypoint
// ---------------------------------------------------------------------------

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pingCtx, cancelPing := context.WithTimeout(ctx, 10*time.Second)
	err = db.PingContext(pingCtx)
	cancelPing()
	if err != nil {
		log.Fatalf("ping database: %v", err)
	}

	client := &http.Client{Timeout: cfg.HTTPTimeout}

	w := &worker{
		cfg:    cfg,
		db:     db,
		client: client,
		tokens: newTokenManager(cfg, client),
	}

	w.runOnce(ctx)

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("shutdown signal received, stopping")
			return
		case <-ticker.C:
			w.runOnce(ctx)
		}
	}
}