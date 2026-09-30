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

// The SSE transport tests exercise it directly against an httptest server — dial,
// read (data payload), the opaque Last-Event-ID cursor (captured at dispatch, echoed
// on reconnect), the idle watchdog, the receive-only Send, and handshake capture.
// goleak (TestMain) proves the watchdog and cancel goroutines are cleaned up.

// sseTransportFor builds an SSE transport pointed at srv, dialing through its client.
func sseTransportFor(t *testing.T, srv *httptest.Server, channels []string) *sseTransport {
	t.Helper()
	cfg := defaultConfig()
	cfg.httpClient = srv.Client()
	return newSSETransport(srv.URL, cfg, nil, func() []string { return channels })
}

func sseEvent(id, data string) string {
	b := &strings.Builder{}
	if id != "" {
		fmt.Fprintf(b, "id: %s\n", id)
	}
	fmt.Fprintf(b, "event: message\ndata: %s\n\n", data)
	return b.String()
}

func TestSSETransportDialsAndReadsDataPayload(t *testing.T) {
	var gotURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.String()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, sseEvent("", `{"type":"message","channel":"t.a","data":{"x":1}}`))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	tr := sseTransportFor(t, srv, []string{"t.a", "t.b"})
	conn, err := tr.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(0, "done") }()

	got, err := conn.Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !strings.Contains(string(got), `"channel":"t.a"`) {
		t.Errorf("Read = %q, want the envelope data payload", got)
	}
	if !strings.Contains(gotURL, "channels=t.a%2Ct.b") {
		t.Errorf("dial URL = %q, want channels=t.a,t.b", gotURL)
	}
}

func TestSSETransportEchoesCursorOnReconnect(t *testing.T) {
	const cursor = "v1:OPAQUE"
	var mu sync.Mutex
	var lastEventIDs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n := len(lastEventIDs) + 1
		lastEventIDs = append(lastEventIDs, r.Header.Get("Last-Event-ID"))
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if n == 1 {
			// A bare id: block (commits the cursor, no data) then a data event with NO id
			// (must not clobber the committed cursor), then the stream ends.
			fmt.Fprintf(w, "id: %s\n\n", cursor)
			fmt.Fprint(w, sseEvent("", `{"type":"message","channel":"t.a","data":{}}`))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			return
		}
		fmt.Fprint(w, sseEvent("", `{"type":"message","channel":"t.a","data":{}}`))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	tr := sseTransportFor(t, srv, []string{"t.a"})

	// Epoch 1: read the data event; the bare-id block committed the cursor.
	conn1, err := tr.Open(context.Background())
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}
	if _, err := conn1.Read(context.Background()); err != nil {
		t.Fatalf("Read 1: %v", err)
	}
	// The stream ended after the event → the next Read returns a CloseError.
	if _, err := conn1.Read(context.Background()); err == nil {
		t.Fatal("Read 1b: expected a close error after the stream ended")
	}
	_ = conn1.Close(0, "")

	// Epoch 2 (reconnect): the request must echo the committed cursor.
	conn2, err := tr.Open(context.Background())
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	defer func() { _ = conn2.Close(0, "") }()
	if _, err := conn2.Read(context.Background()); err != nil {
		t.Fatalf("Read 2: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(lastEventIDs) < 2 {
		t.Fatalf("expected 2 connects, got %d", len(lastEventIDs))
	}
	if lastEventIDs[0] != "" {
		t.Errorf("first connect Last-Event-ID = %q, want none", lastEventIDs[0])
	}
	if lastEventIDs[1] != cursor {
		t.Errorf("reconnect Last-Event-ID = %q, want %q (bare-id committed, id-less event did not clobber)", lastEventIDs[1], cursor)
	}
}

func TestSSETransportMultiLineDataJoined(t *testing.T) {
	env := `{"type":"message","channel":"t.a","data":{"n":1}}`
	cut := strings.Index(env, `,"data"`) + 1
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "event: message\ndata: %s\ndata: %s\n\n", env[:cut], env[cut:])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	tr := sseTransportFor(t, srv, []string{"t.a"})
	conn, err := tr.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(0, "") }()
	got, err := conn.Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != env[:cut]+"\n"+env[cut:] {
		t.Errorf("Read = %q, want the two data lines joined with a newline", got)
	}
}

func TestSSETransportIdleTimeoutCloses(t *testing.T) {
	clock := newFakeClock()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush() // send headers so Open returns; then no bytes ever
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	cfg := defaultConfig()
	cfg.httpClient = srv.Client()
	cfg.clock = clock
	cfg.sseIdleTimeout = 90 * time.Second
	tr := newSSETransport(srv.URL, cfg, nil, func() []string { return []string{"t.a"} })

	conn, err := tr.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(0, "") }()

	readErr := make(chan error, 1)
	go func() { _, e := conn.Read(context.Background()); readErr <- e }()

	clock.BlockUntilTimer(purposeSSEIdle) // wait for the watchdog to arm
	clock.Advance(90 * time.Second)       // fire it → watchdog closes the body

	select {
	case e := <-readErr:
		if _, ok := errors.AsType[*CloseError](e); !ok {
			t.Fatalf("Read error = %v, want a *CloseError from the idle close", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read did not return after the idle timeout fired")
	}
}

func TestSSETransportSendIsReceiveOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	tr := sseTransportFor(t, srv, []string{"t.a"})
	conn, err := tr.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(0, "") }()
	if err := conn.Send(context.Background(), []byte("x")); !errors.Is(err, ErrUnsupportedByTransport) {
		t.Errorf("Send err = %v, want ErrUnsupportedByTransport", err)
	}
}

func TestSSETransportNon200IsHandshakeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"code":"EDITION_LIMIT","message":"SSE is Pro"}`, http.StatusForbidden)
	}))
	defer srv.Close()

	tr := sseTransportFor(t, srv, []string{"t.a"})
	_, err := tr.Open(context.Background())
	he, ok := errors.AsType[*HandshakeError](err)
	if !ok {
		t.Fatalf("Open err = %v, want *HandshakeError", err)
	}
	if he.Status != http.StatusForbidden {
		t.Errorf("HandshakeError.Status = %d, want 403", he.Status)
	}
}
