//go:build e2e

package e2e

import (
	"io"
	"net"
	"sync"
	"testing"
)

// gateProxy is a controllable TCP relay in front of the gateway — the live-stack
// equivalent of sdk-py's gated transport factory. The subscriber dials the proxy; the
// proxy relays to the real gateway. cut() drops the active connection and HOLDS new dials
// (the client's reconnect upgrade blocks in the kernel accept buffer) so messages
// published meanwhile land while the subscriber is genuinely offline; restore() releases
// the held dials, the handshake completes, and the SDK replays from its last position.
//
// Holding — rather than refusing — the reconnect dial mirrors how the SDK's own unit
// tests drive the real transport against a fake gateway: it avoids depending on how a
// failed dial is classified by the reconnect policy and on how backoff grows across
// refused attempts. The connection simply waits until the window closes.
type gateProxy struct {
	ln       net.Listener
	upstream string

	mu    sync.Mutex
	gate  chan struct{}         // closed ⇒ dials pass; open (unclosed) ⇒ dials block (held)
	conns map[net.Conn]struct{} // live relay conns, closed on cut
}

// newGateProxy starts a relay on an ephemeral loopback port forwarding to upstream
// (host:port, e.g. "localhost:13000"). It starts open: dials pass straight through.
func newGateProxy(t *testing.T, upstream string) *gateProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("gateProxy listen: %v", err)
	}
	open := make(chan struct{})
	close(open) // start open
	p := &gateProxy{ln: ln, upstream: upstream, gate: open, conns: map[net.Conn]struct{}{}}
	go p.acceptLoop()
	return p
}

func (p *gateProxy) addr() string { return p.ln.Addr().String() }

func (p *gateProxy) acceptLoop() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return // listener closed
		}
		go p.handle(client)
	}
}

func (p *gateProxy) handle(client net.Conn) {
	// Wait while held: a cut() between accept and here parks the upgrade until restore().
	p.mu.Lock()
	gate := p.gate
	p.mu.Unlock()
	<-gate

	up, err := net.Dial("tcp", p.upstream)
	if err != nil {
		_ = client.Close()
		return
	}
	p.track(client, up)
	defer p.untrack(client, up)

	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		done <- struct{}{}
	}
	go cp(up, client)
	go cp(client, up)
	<-done // one side closed ⇒ tear the pair down; the other cp returns once both close
	_ = client.Close()
	_ = up.Close()
}

func (p *gateProxy) track(cs ...net.Conn) {
	p.mu.Lock()
	for _, c := range cs {
		p.conns[c] = struct{}{}
	}
	p.mu.Unlock()
}

func (p *gateProxy) untrack(cs ...net.Conn) {
	p.mu.Lock()
	for _, c := range cs {
		delete(p.conns, c)
	}
	p.mu.Unlock()
}

// cut drops every live relay connection and holds new dials until restore.
func (p *gateProxy) cut() {
	p.mu.Lock()
	p.gate = make(chan struct{}) // unclosed ⇒ handlers block on it
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
}

// restore releases held dials; subsequent handlers relay immediately.
func (p *gateProxy) restore() {
	p.mu.Lock()
	select {
	case <-p.gate: // already open (closed channel) — nothing to do
	default:
		close(p.gate)
	}
	p.mu.Unlock()
}

func (p *gateProxy) close() { _ = p.ln.Close() }
