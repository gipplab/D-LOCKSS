package shard

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/pinset"
)

// The placement pass is the node's one repair loop. Every pinned file is
// periodically re-evaluated against a single question — "where does this
// file belong now?" — which subsumes what used to be three separate passes:
//
//   - custodial retry: files ingested for a foreign shard are nudged there
//     (ReplicationRequest) until enough providers exist, then released;
//   - orphan handoff: once a child shard is populated, old files of this
//     shard belong one level deeper and are handed down the same way;
//   - reshard: after a move, files outside the new shard are foreign again
//     and take the same handoff path, while newly owned files are covered by
//     the replication checker's regular broadcasts.
//
// All nudges are ReplicationRequests (never IngestMessages, which peers
// accept only from authorized ingesters), and a copy is released only when
// the DHT confirms enough other providers AND at least two nudges were sent
// in earlier passes, so one stale DHT read can never orphan a file.
const (
	placementInterval       = 2 * time.Minute
	placementMinPinAge      = 2 * time.Minute  // leave fresh pins alone
	placementRecheckAfter   = 30 * time.Minute // per-file DHT lookup spacing
	placementLookupsPerPass = 10               // DHT budget per pass
	placementMaxSends       = 100              // nudge budget per pass
	placementHandoffGrace   = 6 * time.Minute  // spacing between nudges per file
	minHandoffsBeforeRelease = 2
	childProbeTimeout        = 4 * time.Second
	postMoveFirstPass        = 5 * time.Second
)

type handoffState struct {
	count int
	last  time.Time
}

// runPlacement drives the pass on its interval; moveToShard schedules an
// early one.
func (m *Manager) runPlacement() {
	for m.sleep(placementInterval) {
		m.placementPass()
	}
}

func (m *Manager) placementPass() {
	// Single flight: the post-move trigger may coincide with the interval.
	if !m.placeBusy.CompareAndSwap(false, true) {
		return
	}
	defer m.placeBusy.Store(false)

	current := m.CurrentShard()
	minPeers := m.cfg.MinPeersPerShard

	// Forget state of files that are gone, and nudge records gone quiet.
	m.placeMu.Lock()
	for k := range m.placeChecked {
		if !m.store.IsPinned(k) {
			delete(m.placeChecked, k)
		}
	}
	for k, h := range m.handoffs {
		if !m.store.IsPinned(k) || time.Since(h.last) > 2*placementHandoffGrace {
			delete(m.handoffs, k)
		}
	}
	m.placeMu.Unlock()

	// One probe per child per pass tells us whether files should move down.
	child0, child1 := Children(current)
	childActive := map[string]bool{
		child0: m.probeShard(child0, childProbeTimeout) >= 1,
		child1: m.probeShard(child1, childProbeTimeout) >= 1,
	}

	// Candidates: old enough, not recently checked; never-checked first,
	// then least recently checked, ties by pin age then key.
	type candidate struct {
		k        cid.Cid
		pinTime  time.Time
		checked  time.Time
		hasCheck bool
	}
	now := time.Now()
	var cands []candidate
	for _, k := range m.store.Pinned() {
		pinTime, known := m.store.PinTime(k)
		if known && now.Sub(pinTime) < placementMinPinAge {
			continue
		}
		if !known {
			pinTime = time.Time{}
		}
		m.placeMu.Lock()
		checked, hasCheck := m.placeChecked[k]
		m.placeMu.Unlock()
		if hasCheck && now.Sub(checked) < placementRecheckAfter {
			continue
		}
		cands = append(cands, candidate{k: k, pinTime: pinTime, checked: checked, hasCheck: hasCheck})
	}
	sort.Slice(cands, func(i, j int) bool {
		a, b := cands[i], cands[j]
		if a.hasCheck != b.hasCheck {
			return !a.hasCheck
		}
		if !a.checked.Equal(b.checked) {
			return a.checked.Before(b.checked)
		}
		if !a.pinTime.Equal(b.pinTime) {
			return a.pinTime.Before(b.pinTime)
		}
		return a.k.KeyString() < b.k.KeyString()
	})

	lookups, sends := 0, 0
	for _, c := range cands {
		select {
		case <-m.ctx.Done():
			return
		default:
		}
		if lookups >= placementLookupsPerPass {
			break
		}
		rctx, cancel := context.WithTimeout(m.ctx, payloadResolveTimeout)
		ro, err := pinset.ReadManifest(rctx, m.client, c.k)
		cancel()
		m.placeMu.Lock()
		m.placeChecked[c.k] = time.Now()
		m.placeMu.Unlock()
		if err != nil || !ro.Payload.Defined() {
			continue // marked checked, no lookup slot used
		}
		lookups++
		key := ro.Payload.String()

		// Where does the file belong now? Outside our prefix: its nominal
		// shard. Inside: a populated child when one covers it, else here.
		home := current
		if !Responsible(current, key) {
			home = TargetShardForPayload(key, max(1, len(current)))
		} else if child := TargetShardForPayload(key, len(current)+1); childActive[child] {
			home = child
		}

		pctx, cancel := context.WithTimeout(m.ctx, handoffConfirmTimeout)
		providers := m.store.ProviderCount(pctx, c.k, m.self, minPeers)
		cancel()

		if home == current {
			// Our file: if the network holds too few other copies, ask the
			// shard to converge on it.
			if providers >= max(1, minPeers-1) || sends >= placementMaxSends {
				continue
			}
			sends++
			if _, err := m.pins.AddIfAbsent(current, c.k); err == nil {
				if err := m.sendReplicationRequest(current, c.k, false); err != nil {
					slog.Debug("placement: reannounce failed", "cid", c.k, "err", err)
				}
			}
			continue
		}

		// Foreign (or belongs a level deeper): release once confirmed and
		// nudged enough, otherwise nudge its home shard.
		m.placeMu.Lock()
		h := m.handoffs[c.k]
		if h == nil {
			h = &handoffState{}
			m.handoffs[c.k] = h
		}
		nudgedEnough := h.count >= minHandoffsBeforeRelease
		waiting := !h.last.IsZero() && time.Since(h.last) < placementHandoffGrace
		m.placeMu.Unlock()

		if providers >= minPeers && nudgedEnough {
			slog.Info("placement: handoff confirmed, releasing copy", "cid", c.k, "home", shardLabel(home))
			m.unpinEverywhere(c.k)
			m.placeMu.Lock()
			delete(m.handoffs, c.k)
			m.placeMu.Unlock()
			continue
		}
		if waiting || sends >= placementMaxSends {
			continue
		}
		sends++
		if err := m.sendReplicationRequest(home, c.k, true); err != nil {
			slog.Debug("placement: handoff send failed", "cid", c.k, "shard", shardLabel(home), "err", err)
			continue
		}
		m.placeMu.Lock()
		h.count++
		h.last = time.Now()
		count := h.count
		m.placeMu.Unlock()
		slog.Info("placement: nudged home shard", "cid", c.k, "home", shardLabel(home), "nudges", count)
	}
}
