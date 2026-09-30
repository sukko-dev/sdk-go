package sukko

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// sseTestServer is an httptest SSE server that records the ?channels= of each connection and
// holds each stream open until the client closes it (a bounce) or the test ends.
type sseTestServer struct {
	*httptest.Server
	mu       sync.Mutex
	connects [][]string // channels seen on each connect, in order
}

func newSSETestServer(t *testing.T) *sseTestServer {
	t.Helper()
	s := &sseTestServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chans := strings.Split(r.URL.Query().Get("channels"), ",")
		if len(chans) == 1 && chans[0] == "" {
			chans = nil
		}
		s.mu.Lock()
		s.connects = append(s.connects, chans)
		s.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			_, _ = w.Write([]byte(": ka\n\n")) // best-effort keepalive so Open returns
			f.Flush()
		}
		<-r.Context().Done() // hold open until the client closes (bounce) or the test ends
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *sseTestServer) connectCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.connects)
}

func (s *sseTestServer) channelsAt(i int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connects[i]
}

func newSSEClient(t *testing.T, s *sseTestServer) *Client {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(s.URL, "http") + "/ws" // ws://host:port/ws → restBase http://host:port
	c, err := NewClient(context.Background(), wsURL,
		WithToken("jwt"), WithTransport(TransportSSE),
		WithHTTPClient(s.Client()), WithInsecureTransport())
	if err != nil {
		t.Fatalf("NewClient(SSE): %v", err)
	}
	return c
}

func waitConnects(t *testing.T, s *sseTestServer, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s.connectCount() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %d connects (got %d)", n, s.connectCount())
}

func TestSSEClient_SubscribeUnsubscribeBounceAndPark(t *testing.T) {
	s := newSSETestServer(t)
	c := newSSEClient(t, s)
	defer func() { _ = c.Close(context.Background()) }()

	// Subscribe before connect is recorded; Connect dials with it.
	if err := c.Subscribe(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("Subscribe(a): %v", err)
	}
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	waitConnects(t, s, 1)
	if got := s.channelsAt(0); len(got) != 1 || got[0] != "a" {
		t.Fatalf("connect 1 channels = %v, want [a]", got)
	}

	// subscribe on live → bounce → redial with the union.
	if err := c.Subscribe(context.Background(), []string{"b"}); err != nil {
		t.Fatalf("Subscribe(b): %v", err)
	}
	waitConnects(t, s, 2)
	if got := s.channelsAt(1); strings.Join(got, ",") != "a,b" {
		t.Fatalf("connect 2 (bounce) channels = %v, want [a b]", got)
	}

	// unsubscribe on live → bounce → redial with the reduced set.
	if err := c.Unsubscribe(context.Background(), []string{"b"}); err != nil {
		t.Fatalf("Unsubscribe(b): %v", err)
	}
	waitConnects(t, s, 3)
	if got := s.channelsAt(2); len(got) != 1 || got[0] != "a" {
		t.Fatalf("connect 3 channels = %v, want [a]", got)
	}

	// unsubscribe the last channel → park (no redial), then subscribe → wakes and dials.
	if err := c.Unsubscribe(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("Unsubscribe(a): %v", err)
	}
	time.Sleep(150 * time.Millisecond) // give a wrong redial a chance to appear
	if got := s.connectCount(); got != 3 {
		t.Fatalf("parked: connect count = %d, want 3 (no dial into an empty channel set)", got)
	}
	if err := c.Subscribe(context.Background(), []string{"c"}); err != nil {
		t.Fatalf("Subscribe(c): %v", err)
	}
	waitConnects(t, s, 4)
	if got := s.channelsAt(3); len(got) != 1 || got[0] != "c" {
		t.Fatalf("connect 4 (wake) channels = %v, want [c]", got)
	}
}

func TestSSEClient_PublishIsReceiveOnly(t *testing.T) {
	s := newSSETestServer(t)
	c := newSSEClient(t, s)
	defer func() { _ = c.Close(context.Background()) }()

	if err := c.Subscribe(context.Background(), []string{"a"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	waitConnects(t, s, 1)

	err := c.Publish(context.Background(), "a", map[string]any{"x": 1})
	if !errors.Is(err, ErrUnsupportedByTransport) {
		t.Errorf("Publish over SSE = %v, want ErrUnsupportedByTransport", err)
	}
	if got := c.Subscriptions(); len(got) != 1 || got[0] != "a" {
		t.Errorf("Subscriptions() = %v, want [a] (the connect-time desired set)", got)
	}
}

func TestSSEClient_SubscribeDuringDialWindowIsNotLost(t *testing.T) {
	var mu sync.Mutex
	var connects [][]string
	proceed := make(chan struct{})
	gated := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chans := strings.Split(r.URL.Query().Get("channels"), ",")
		mu.Lock()
		connects = append(connects, chans)
		n := len(connects)
		g := gated
		mu.Unlock()
		if n >= 2 && g {
			<-proceed // block this connect BEFORE headers → the client's Open blocks (dial window)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			_, _ = w.Write([]byte(": ka\n\n"))
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()
	s := &sseTestServer{Server: srv}
	c := newSSEClient(t, s)
	defer func() { _ = c.Close(context.Background()) }()

	_ = c.Subscribe(context.Background(), []string{"a"})
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(connects) >= 1 })

	mu.Lock()
	gated = true
	mu.Unlock()
	_ = c.Subscribe(context.Background(), []string{"b"}) // bounce → connect 2 starts, blocks in the dial window
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(connects) >= 2 })
	_ = c.Subscribe(context.Background(), []string{"c"}) // lands DURING connect 2's dial window (currentConn nil)
	mu.Lock()
	gated = false
	mu.Unlock()
	close(proceed) // connect 2 completes with [a b]; the reconcile detects the mismatch and bounces

	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(connects) >= 3 })
	mu.Lock()
	got := connects[2]
	mu.Unlock()
	if strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("connect 3 channels = %v, want [a b c] — a dial-window subscribe was lost", got)
	}
}

func TestSSEClient_RefreshAndEscalateAreUnsupported(t *testing.T) {
	s := newSSETestServer(t)
	c := newSSEClient(t, s)
	defer func() { _ = c.Close(context.Background()) }()
	_ = c.Subscribe(context.Background(), []string{"a"})
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	waitConnects(t, s, 1)

	if err := c.RefreshToken(context.Background()); !errors.Is(err, ErrUnsupportedByTransport) {
		t.Errorf("RefreshToken over SSE = %v, want ErrUnsupportedByTransport", err)
	}
	if err := c.Escalate(context.Background(), "jwt"); !errors.Is(err, ErrUnsupportedByTransport) {
		t.Errorf("Escalate over SSE = %v, want ErrUnsupportedByTransport", err)
	}
	if p := c.PendingSubscriptions(); len(p) != 0 {
		t.Errorf("PendingSubscriptions() over SSE = %v, want empty (nothing is pending; desired IS subscribed)", p)
	}
}

// waitFor polls cond until true or a 3s deadline.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("waitFor: condition not met within 3s")
}
