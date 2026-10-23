//go:build e2e

package e2e

import (
	"context"
	"testing"

	sukko "github.com/sukko-dev/sdk-go"
)

// TestPushVAPIDKey fetches the tenant's Web Push VAPID public key (Web Push = Pro). It is
// skipped unless the live stack is Pro+ — on the Community stack the push route is absent,
// so the capability cannot be exercised here; the skip-with-reason records the gap rather
// than silently omitting it. A Pro/Enterprise stack (SUKKO_IT_EDITION) exercises it.
func TestPushVAPIDKey(t *testing.T) {
	requireLive(t)
	if envEdition != "pro" && envEdition != "enterprise" {
		t.Skipf("Web Push is Pro-gated — stack edition is %q", envEdition)
	}
	ctx := context.Background()

	c := newClient(t, envWSURL, sukko.WithToken(envToken))
	defer func() { _ = c.Close(ctx) }()

	key, err := c.PushVAPIDKey(ctx)
	if err != nil {
		t.Fatalf("PushVAPIDKey: %v", err)
	}
	if key == "" {
		t.Error("PushVAPIDKey returned an empty key")
	}
}
