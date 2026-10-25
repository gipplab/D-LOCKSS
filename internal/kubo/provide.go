package kubo

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
)

// provideTimeout bounds one announce when the caller passed no deadline.
const provideTimeout = time.Minute

// Provide announces the CID to the DHT. Kubo >= 0.43's provide/once queues
// the CID with the daemon's own provider and returns, so no client-side
// queueing, retries, or legacy routing/provide fallback is needed.
func (c *Client) Provide(ctx context.Context, k cid.Cid) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, provideTimeout)
		defer cancel()
	}
	return c.provideOnce(ctx, k)
}
