package sukko

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// The SSE transport, the receive-only counterpart to transport_ws.go. It dials
// GET {rest-base}/sse?channels=… through the caller's *http.Client (the same
// TLS/CA/proxy seam), reads the text/event-stream, and returns each event's data:
// payload — the full message envelope — so the dispatch layer is unchanged from
// WebSocket. Recovery is server-driven: the connection carries the opaque
// Last-Event-ID cursor it last saw and re-sends it on the next Open, so the server
// replays the gap (ADR-0015). There is no client→server frame — Send is a typed
// error — so this transport is far simpler than the WebSocket one; its only moving
// parts are the SSE line parser and the idle watchdog.

// sseReadBufferOverhead mirrors wsEnvelopeOverhead: the scanner's max line size is
// MaxPublishSize plus envelope headroom, so a full-size message arrives intact
// rather than tripping bufio.Scanner's token-too-long error.
const sseReadBufferOverhead = 4 * 1024

// sseAbnormalCloseCode is WebSocket close code 1006 (abnormal closure) — the SDK's
// CloseError classifier reconnects a {1006, local} drop. SSE has no close frame, so a
// stream end, an idle timeout, or a read error is reported as this local abnormal
// close (matching the WebSocket transport's abnormal-drop path) rather than importing
// the websocket library for the constant.
const sseAbnormalCloseCode = 1006

// sseTransport dials SSE connections. Built once (like wsTransport); the cursor
// rides on it across epochs.
type sseTransport struct {
	baseURL     string // http(s):// origin derived from the ws:// client URL
	httpClient  *http.Client
	clock       Clock
	idleTimeout time.Duration
	// credentials returns the current credential pair, read per dial (rotation aware);
	// nil under WithNoAuth. channels returns the connect-time channel set, read per dial
	// (SSE subscribes via the URL — the client's desired set at connect time).
	credentials    func() (token, apiKey string)
	channels       func() []string
	queryParamAuth bool
	readLimit      int

	mu        sync.Mutex
	lastEvent string // opaque Last-Event-ID cursor; persists across epochs (ADR-0015)
}

// newSSETransport builds an SSE transport. baseURL is the HTTP origin (restBaseURL of
// the ws:// client URL); credentials and channels are read at every dial.
func newSSETransport(baseURL string, cfg *config, credentials func() (token, apiKey string), channels func() []string) *sseTransport {
	return &sseTransport{
		baseURL:        baseURL,
		httpClient:     cfg.httpClient,
		clock:          cfg.clock,
		idleTimeout:    cfg.sseIdleTimeout,
		credentials:    credentials,
		channels:       channels,
		queryParamAuth: cfg.queryParamAuth,
		readLimit:      cfg.maxPublishSize + sseReadBufferOverhead,
	}
}

func (t *sseTransport) Capabilities() Capabilities { return capabilitiesFor(TransportSSE) }

func (t *sseTransport) cursor() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastEvent
}

func (t *sseTransport) setCursor(id string) {
	t.mu.Lock()
	t.lastEvent = id
	t.mu.Unlock()
}

// buildURL assembles GET {base}/sse?channels=…[&token=…], applying query-param auth
// when configured. Returns the URL and the header set (Authorization / X-API-Key /
// Last-Event-ID) for the request.
func (t *sseTransport) buildURL() (string, http.Header, error) {
	u, err := url.Parse(t.baseURL)
	if err != nil {
		return "", nil, fmt.Errorf("sukko: sse base url: %w", err)
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/sse"
	q := u.Query()
	if chs := t.channels(); len(chs) > 0 {
		q.Set("channels", strings.Join(chs, ","))
	}
	header := http.Header{}
	if t.credentials != nil {
		token, apiKey := t.credentials()
		switch {
		case t.queryParamAuth:
			if token != "" {
				q.Set("token", token)
			}
			if apiKey != "" {
				q.Set("api_key", apiKey)
			}
		default:
			if token != "" {
				header.Set(headerAuthorization, authBearerPrefix+token)
			}
			if apiKey != "" {
				header.Set(headerAPIKey, apiKey)
			}
		}
	}
	u.RawQuery = q.Encode()
	if c := t.cursor(); c != "" {
		header.Set("Last-Event-ID", c) // opaque — echoed verbatim (ADR-0015)
	}
	return u.String(), header, nil
}

// Open dials the SSE stream. A non-200 is surfaced as a typed *HandshakeError (the
// Pro gate's 403 EDITION_LIMIT included); success returns an sseConn whose idle
// watchdog is already running.
func (t *sseTransport) Open(ctx context.Context) (Conn, error) {
	dialURL, header, err := t.buildURL()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dialURL, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("sukko: sse request: %w", err)
	}
	req.Header = header
	req.Header.Set("Accept", "text/event-stream")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sukko: sse dial: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		he := handshakeErrorFromResponse(resp, t.clock.Now())
		_ = resp.Body.Close()
		return nil, he
	}

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), t.readLimit)
	c := &sseConn{
		transport: t,
		resp:      resp,
		scanner:   sc,
		done:      make(chan struct{}),
		activity:  make(chan struct{}, 1),
	}
	c.startWatchdog(t.clock, t.idleTimeout)
	return c, nil
}

// sseConn is one live SSE connection.
type sseConn struct {
	transport *sseTransport
	resp      *http.Response
	scanner   *bufio.Scanner
	closeOnce sync.Once
	done      chan struct{} // closed on Close; stops the watchdog
	activity  chan struct{} // pinged on every scanned line; resets the idle timer
}

// startWatchdog runs the idle watchdog: the stream must deliver at least one line
// (event, keepalive comment, or blank) within idleTimeout, else the connection is
// closed and the next Read surfaces a local abnormal CloseError so the supervisor
// reconnects. A zero/negative timeout disables the watchdog.
func (c *sseConn) startWatchdog(clock Clock, idle time.Duration) {
	if idle <= 0 {
		return
	}
	go func() {
		timer := clock.NewTimer(idle, purposeSSEIdle)
		defer timer.Stop()
		for {
			select {
			case <-c.done:
				return
			case <-c.activity:
				timer.Stop()
				timer = clock.NewTimer(idle, purposeSSEIdle)
			case <-timer.C():
				_ = c.Close(sseAbnormalCloseCode, "sse idle timeout")
				return
			}
		}
	}()
}

// Read returns the next event's data payload — the full JSON envelope — so the
// dispatch layer treats it exactly like a WebSocket frame. It captures the opaque
// Last-Event-ID cursor at event dispatch (the blank line, per WHATWG EventSource —
// never at the id: line, so a truncated block cannot advance the cursor past an
// event never delivered). Keepalive comments are skipped; every scanned line pings
// the idle watchdog. A stream end or idle-close is surfaced as a typed *CloseError.
func (c *sseConn) Read(ctx context.Context) ([]byte, error) {
	// Cancellation helper: close the body on ctx.Done so a blocked Scan unblocks.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = c.Close(sseAbnormalCloseCode, "context canceled")
		case <-stop:
		}
	}()

	var data strings.Builder
	var lastEventID string
	hasSeenID := false
	hasData := false

	for c.scanner.Scan() {
		select {
		case c.activity <- struct{}{}:
		default: // watchdog already has a pending ping — coalesce
		}
		line := c.scanner.Text()
		switch {
		case line == "":
			// Dispatch: commit the block's last-event-id (even for a data-less bare
			// id: block — the gateway's keepalive-flush), then return if it carried data.
			if hasSeenID {
				c.transport.setCursor(lastEventID)
			}
			if hasData {
				return []byte(data.String()), nil
			}
			data.Reset()
			hasSeenID = false
		case strings.HasPrefix(line, ":"):
			// keepalive comment — skip (it still pinged the watchdog above)
		case strings.HasPrefix(line, "id:"):
			lastEventID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
			hasSeenID = true
		case strings.HasPrefix(line, "data:"):
			if hasData {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			hasData = true
		}
	}

	if err := c.scanner.Err(); err != nil && ctx.Err() == nil {
		return nil, &CloseError{Code: sseAbnormalCloseCode, Direction: directionLocal, Reason: err.Error()}
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("sukko: sse read canceled: %w", ctx.Err())
	}
	// Clean EOF (server ended the stream) or an idle-close: an abnormal local drop
	// the supervisor reconnects from.
	return nil, &CloseError{Code: sseAbnormalCloseCode, Direction: directionLocal, Reason: "sse stream ended"}
}

// Send is a typed error: SSE is receive-only. The client gates publish/subscribe on
// Capabilities before ever calling this, so it is a defensive backstop.
func (c *sseConn) Send(context.Context, []byte) error {
	return ErrUnsupportedByTransport
}

// Close stops the watchdog and closes the response body, exactly once.
func (c *sseConn) Close(int, string) error {
	var err error
	c.closeOnce.Do(func() {
		close(c.done)
		if e := c.resp.Body.Close(); e != nil {
			err = fmt.Errorf("sukko: sse close: %w", e)
		}
	})
	return err
}
