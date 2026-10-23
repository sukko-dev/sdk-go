package sukko

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// The real gateway accepts an SSE stream (200, text/event-stream, headers flushed) and then
// sends NOTHING until the first message or its keepalive — far longer than a dial. The fake
// servers in the other SSE tests all write a byte on connect, which hid a transport bug: the
// stream must outlive the dial context the supervisor cancels the moment Open returns. These
// two tests pin the silent-start path at both layers.

// silentSSEServer accepts SSE connections, flushes the 200 headers, and stays silent until
// release is closed; it then writes one event and holds the stream open.
type silentSSEServer struct {
	*httptest.Server
	release  chan struct{}
	mu       sync.Mutex
	connects int
}

func newSilentSSEServer(t *testing.T, event string) *silentSSEServer {
	t.Helper()
	s := &silentSSEServer{release: make(chan struct{})}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.connects++
		s.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush() // headers only — the real gateway's silent start
		}
		select {
		case <-s.release:
		case <-r.Context().Done():
			return
		}
		fmt.Fprint(w, event)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *silentSSEServer) connectCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connects
}

// TestSSETransportStreamOutlivesDialContext pins the mechanism: the ctx passed to Open bounds the
// dial only (as the WebSocket transport's does). Canceling it after Open returns must not tear
// down the stream — a later event is still read.
func TestSSETransportStreamOutlivesDialContext(t *testing.T) {
	srv := newSilentSSEServer(t, sseEvent("", `{"type":"message","channel":"t.a","data":{"x":1}}`))
	tr := sseTransportFor(t, srv.Server, []string{"t.a"})

	dialCtx, dialCancel := context.WithCancel(context.Background())
	conn, err := tr.Open(dialCtx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(0, "done") }()
	dialCancel() // the supervisor does exactly this the moment Open returns

	close(srv.release)
	got, err := conn.Read(context.Background())
	if err != nil {
		t.Fatalf("Read after dial-ctx cancel: %v (the stream must outlive the dial context)", err)
	}
	if !strings.Contains(string(got), `"channel":"t.a"`) {
		t.Errorf("Read = %q, want the envelope data payload", got)
	}
}

// TestSSEClient_SilentStartStaysConnectedAndDelivers pins the user-visible behavior: a client on
// a healthy-but-silent SSE stream stays Connected on its first epoch (no reconnect loop) and
// delivers the first event when the server finally speaks.
func TestSSEClient_SilentStartStaysConnectedAndDelivers(t *testing.T) {
	srv := newSilentSSEServer(t, sseEvent("", `{"type":"message","channel":"t.a","data":{"probe":"sse"}}`))
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	c, err := NewClient(context.Background(), wsURL,
		WithToken("jwt"), WithTransport(TransportSSE),
		WithHTTPClient(srv.Client()), WithInsecureTransport())
	if err != nil {
		t.Fatalf("NewClient(SSE): %v", err)
	}
	defer func() { _ = c.Close(context.Background()) }()

	if err := c.Subscribe(context.Background(), []string{"t.a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// The silent window: the stream is open and healthy, the server just has nothing to say.
	// The client must stay Connected on the one and only connection the whole time. A brief
	// window suffices — the real guard is the timing-free connectCount/Reconnects discriminator
	// at delivery below (pre-fix it tripped on the first iteration at ~0.01s).
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if st := c.State(); st != StateConnected {
			t.Fatalf("state = %v during the silent window, want %v (the epoch was torn down on a healthy stream)", st, StateConnected)
		}
		if n := srv.connectCount(); n != 1 {
			t.Fatalf("connects = %d during the silent window, want 1 (reconnect loop)", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := c.Stats().Reconnects; n != 0 {
		t.Fatalf("Reconnects = %d, want 0", n)
	}

	close(srv.release)
	timeout := time.After(3 * time.Second)
	for {
		select {
		case ev, ok := <-c.Messages():
			if !ok {
				t.Fatal("Messages() closed before the event")
			}
			if m, isMsg := ev.(*Message); isMsg {
				if m.Channel != "t.a" {
					t.Errorf("Message.Channel = %q, want t.a", m.Channel)
				}
				// Timing-free discriminator: the event must have landed on the FIRST epoch.
				// Pre-fix it could only ever arrive on a redialed one.
				if n := srv.connectCount(); n != 1 {
					t.Errorf("connects = %d at delivery, want 1 (the event arrived on a redialed epoch)", n)
				}
				if n := c.Stats().Reconnects; n != 0 {
					t.Errorf("Reconnects = %d at delivery, want 0", n)
				}
				return
			}
		case <-timeout:
			t.Fatal("timed out waiting for the first event after the silent window")
		}
	}
}

// newHangingSSEServer accepts the connection but never writes a response, so a dial with a
// deadline times out mid-handshake (Do is still in flight when the ctx fires).
func newHangingSSEServer(t *testing.T) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(s.Close)
	return s
}

// TestSSETransportDialDeadlineReportsDeadlineExceeded: a dial-ctx DEADLINE firing mid-handshake
// must surface as context.DeadlineExceeded (as the WebSocket transport does) — not the
// context.Canceled that canceling the stream's own context induces on the request. A caller
// telling "my connect timed out" from "I canceled" via errors.Is must get a transport-independent
// answer.
func TestSSETransportDialDeadlineReportsDeadlineExceeded(t *testing.T) {
	srv := newHangingSSEServer(t)
	tr := sseTransportFor(t, srv, []string{"t.a"})

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	conn, err := tr.Open(ctx)
	if conn != nil {
		_ = conn.Close(0, "")
		t.Fatal("Open returned a conn on a dial timeout; want nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Open err = %v; want errors.Is(context.DeadlineExceeded) — a dial deadline must not surface as Canceled", err)
	}
}

// cancelAfterRoundTripper fires cancel the instant the real round trip returns — before Open
// regains control — to deterministically hit the !dialLive window (the dial ctx canceled between
// Do returning and stopPropagate()).
type cancelAfterRoundTripper struct {
	inner  http.RoundTripper
	cancel context.CancelFunc
}

func (rt *cancelAfterRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := rt.inner.RoundTrip(req)
	rt.cancel()
	return resp, err
}

// TestSSETransportDialCanceledInNonLiveWindow: if the dial ctx is canceled in the window between
// Do returning and Open regaining control, Open returns a dial error (context.Canceled) and no
// live conn — it does not hand back a stream that is already being torn down.
func TestSSETransportDialCanceledInNonLiveWindow(t *testing.T) {
	srv := newSilentSSEServer(t, sseEvent("", `{"type":"message","channel":"t.a","data":{"x":1}}`))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := defaultConfig()
	cfg.httpClient = &http.Client{Transport: &cancelAfterRoundTripper{inner: srv.Client().Transport, cancel: cancel}}
	tr := newSSETransport(srv.URL, cfg, nil, func() []string { return []string{"t.a"} })

	conn, err := tr.Open(ctx)
	if conn != nil {
		_ = conn.Close(0, "")
		t.Fatal("Open returned a live conn in the !dialLive window; want nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Open err = %v; want errors.Is(context.Canceled)", err)
	}
}

// newStalled403Server returns 403 with flushed headers, then holds the response body open
// (never EOFs) until release is closed — a reverse proxy / LB error page that flushes a
// status and stalls. The error-body read in Open must stay bounded by the dial ctx: once the
// stream is detached from the dial ctx (stopPropagate), a naive read would hang Open forever.
func newStalled403Server(t *testing.T) *httptest.Server {
	t.Helper()
	release := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		if f, ok := w.(http.Flusher); ok {
			f.Flush() // status + headers, no body
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	// Cleanups run LIFO: register s.Close first, then close(release), so release fires BEFORE
	// Close — otherwise a RED run (Open still hanging on the body) pins the handler and
	// httptest.Server.Close waits on it, hanging the whole suite.
	t.Cleanup(s.Close)
	t.Cleanup(func() { close(release) })
	return s
}

// TestSSETransportDialDeadlineAbortsStalledErrorBody is the regression guard for the hang that
// the stream-detachment fix introduced: a non-200 whose body never EOFs. The error-body read
// (handshakeErrorFromResponse) runs after the dial ctx is detached from the stream, so it must
// be independently bounded. Open must return a *HandshakeError carrying the real status (not
// hang, and not mask the 403 as a timeout — the supervisor classifies on Status).
func TestSSETransportDialDeadlineAbortsStalledErrorBody(t *testing.T) {
	srv := newStalled403Server(t)
	tr := sseTransportFor(t, srv, []string{"t.a"})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	type result struct {
		conn Conn
		err  error
	}
	done := make(chan result, 1)
	go func() {
		conn, err := tr.Open(ctx)
		done <- result{conn, err}
	}()

	select {
	case r := <-done:
		if r.conn != nil {
			_ = r.conn.Close(0, "")
			t.Fatal("Open returned a conn on a 403; want nil")
		}
		var he *HandshakeError
		if !errors.As(r.err, &he) {
			t.Fatalf("Open err = %v; want a *HandshakeError carrying the 403 status", r.err)
		}
		if he.Status != http.StatusForbidden {
			t.Errorf("HandshakeError.Status = %d, want 403", he.Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Open did not return within 2s after a 200ms dial deadline — the error-body read hung")
	}
}
