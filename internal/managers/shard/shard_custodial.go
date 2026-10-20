package shard

import (
	"log/slog"
	"sort"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/common"
)

const (
	custodialRetryInterval = 2 * time.Minute
	// custodialLookupsPerPass bounds the DHT provider lookups of one pass. A
	// lookup for a file with fewer providers than needed runs until
	// handoffConfirmTimeout, so a pass costs at most this many timeouts.
	custodialLookupsPerPass = 10
	// custodialRecheckAfter is how long a file waits after a lookup before
	// the next one.
	custodialRecheckAfter = 30 * time.Minute
)

// custodialCandidate is a pinned file past its grace period whose last lookup,
// if any, is older than custodialRecheckAfter.
type custodialCandidate struct {
	key     string
	pinTime time.Time
	checked time.Time
}

// orderCustodialCandidates puts never-checked files first, then the least
// recently checked, so every file gets a lookup in turn. Ties go to the oldest
// pin, then the key.
func orderCustodialCandidates(due []custodialCandidate) {
	sort.Slice(due, func(i, j int) bool {
		ci, cj := due[i].checked, due[j].checked
		if !ci.Equal(cj) {
			return ci.Before(cj)
		}
		return pinnedEarlier(due[i].pinTime, due[j].pinTime, due[i].key, due[j].key)
	})
}

// pinnedEarlier orders files by pin time, unknown pin times first, then by key.
func pinnedEarlier(ti, tj time.Time, ki, kj string) bool {
	if ti.IsZero() != tj.IsZero() {
		return ti.IsZero()
	}
	if !ti.Equal(tj) {
		return ti.Before(tj)
	}
	return ki < kj
}

// custodialKind is what one retry pass should do with a file this node still holds.
type custodialKind int

const (
	// custodialRelease drops a copy another shard has confirmed.
	custodialRelease custodialKind = iota
	// custodialReinject sends a tourist ingest again. The local pin stays until confirmation.
	custodialReinject
	// custodialReannounce repeats an own-shard ingest that still has too few providers.
	custodialReannounce
)

// custodialItem is one file eligible for this pass, already past the grace period.
type custodialItem struct {
	key     string
	payload string
	pinTime time.Time
	kind    custodialKind
}

// custodialRetryBudget is how many protocol messages one pass may send.
// Receivers drop a sender after MaxMessagesPerWindow CBOR messages in one
// rate-limit window (default 100 per minute). The pass runs every two minutes,
// so a single window is the most a burst can deliver. A configured zero sends
// nothing; releases do not count against the budget.
func (sm *ShardManager) custodialRetryBudget() int {
	if sm.cfg == nil {
		return 100
	}
	if sm.cfg.MaxMessagesPerWindow < 0 {
		return 0
	}
	return sm.cfg.MaxMessagesPerWindow
}

// orderCustodialWork sorts by pin time, oldest first, and keeps every release.
// Retries stop at budget so a large backlog cannot starve the oldest files and
// cannot exceed the receivers' rate limit. Equal pin times break ties by key.
func orderCustodialWork(items []custodialItem, budget int) []custodialItem {
	sort.Slice(items, func(i, j int) bool {
		return pinnedEarlier(items[i].pinTime, items[j].pinTime, items[i].key, items[j].key)
	})
	if budget < 0 {
		budget = 0
	}
	out := make([]custodialItem, 0, len(items))
	sent := 0
	for _, it := range items {
		switch it.kind {
		case custodialRelease:
			out = append(out, it)
		case custodialReinject, custodialReannounce:
			if sent >= budget {
				continue
			}
			sent++
			out = append(out, it)
		}
	}
	return out
}

// RunCustodialRetryPass handles files this node holds for another shard: an
// ingest whose cross-shard injection was lost (receivers rate-limit senders,
// a fresh observer may publish before the mesh forms), or a copy left from an
// earlier membership. Sending the ingest is not a handoff. The local copy stays
// until handoffConfirmed reports at least MinPeersPerShard other providers;
// until then the file is injected again. The pass sends the oldest files first
// and at most one rate-limit window of messages.
// Files of the node's own shard get the same check: an ingest or a pin
// migration during shard moves can leave a file in one member's CRDT only,
// so a holder that sees fewer than MinPeersPerShard-1 other providers
// announces the file to its shard again.
// Each check is a DHT lookup, so a pass checks at most custodialLookupsPerPass
// files, never-checked and least recently checked first, and a checked file
// waits custodialRecheckAfter before its next lookup.
func (sm *ShardManager) RunCustodialRetryPass() {
	now := time.Now()
	if sm.custodialChecked == nil {
		sm.custodialChecked = make(map[string]time.Time)
	}
	pinned := make(map[string]struct{})
	var due []custodialCandidate
	for key := range sm.storageMgr.GetAllKnownFiles() {
		if !sm.storageMgr.IsPinned(key) {
			continue
		}
		pinned[key] = struct{}{}
		pinTime := sm.storageMgr.GetPinTime(key)
		if !pinTime.IsZero() && now.Sub(pinTime) < custodialRetryInterval {
			continue
		}
		checked := sm.custodialChecked[key]
		if !checked.IsZero() && now.Sub(checked) < custodialRecheckAfter {
			continue
		}
		due = append(due, custodialCandidate{key: key, pinTime: pinTime, checked: checked})
	}
	for key := range sm.custodialChecked {
		if _, ok := pinned[key]; !ok {
			delete(sm.custodialChecked, key)
		}
	}
	orderCustodialCandidates(due)

	var items []custodialItem
	lookups := 0
	for _, c := range due {
		if lookups >= custodialLookupsPerPass {
			break
		}
		if sm.ctx.Err() != nil {
			return
		}
		key := c.key
		sm.custodialChecked[key] = now
		payloadCIDStr, err := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, key)
		if err != nil || payloadCIDStr == key {
			continue
		}
		lookups++
		item := custodialItem{key: key, payload: payloadCIDStr, pinTime: c.pinTime}
		if sm.AmIResponsibleFor(payloadCIDStr) {
			if sm.otherProvidersAtLeast(key, max(1, sm.cfg.Sharding.MinPeersPerShard-1)) {
				continue
			}
			item.kind = custodialReannounce
		} else if sm.handoffConfirmed(key) {
			item.kind = custodialRelease
		} else {
			item.kind = custodialReinject
		}
		items = append(items, item)
	}

	retried, released, reannounced := 0, 0, 0
	for _, it := range orderCustodialWork(items, sm.custodialRetryBudget()) {
		if sm.ctx.Err() != nil {
			return
		}
		switch it.kind {
		case custodialRelease:
			sm.releaseCustodialCopy(it.key, it.payload)
			released++
		case custodialReannounce:
			if sm.reannounceToOwnShard(it.key) {
				reannounced++
			}
		case custodialReinject:
			if sm.reinjectCustodial(it.key, it.payload) {
				retried++
			}
		}
	}
	if retried > 0 || released > 0 || reannounced > 0 {
		slog.Info("custodial retry pass complete", "reinjected", retried, "released", released, "reannounced", reannounced, "checked", lookups, "due", len(due))
	}
}

func (sm *ShardManager) reannounceToOwnShard(key string) bool {
	manifestCID, err := cid.Decode(key)
	if err != nil {
		return false
	}
	currentShard := sm.getCurrentShard()
	if err := sm.clusterMgr.PinIfAbsent(sm.ctx, currentShard, manifestCID, -1, -1); err != nil {
		slog.Error("re-announce: failed to pin to own shard", "shard", currentShard, "error", err)
		return false
	}
	if sm.signer != nil {
		sm.publishIngest(manifestCID, currentShard)
	}
	slog.Info("re-announced under-replicated file to own shard", "shard", currentShard, "manifest", key)
	return true
}

func (sm *ShardManager) releaseCustodialCopy(key, payloadCIDStr string) {
	manifestCID, err := cid.Decode(key)
	if err != nil {
		return
	}
	slog.Info("custodial: releasing copy, responsible shard holds min copies", "manifest", key)
	if err := sm.ipfsClient.UnpinRecursive(sm.ctx, manifestCID); err != nil {
		slog.Debug("custodial: manifest unpin failed", "manifest", key, "error", err)
	}
	if payloadCID, err := cid.Decode(payloadCIDStr); err == nil {
		if err := sm.ipfsClient.UnpinRecursive(sm.ctx, payloadCID); err != nil {
			slog.Debug("custodial: payload unpin failed", "payload", payloadCIDStr, "error", err)
		}
	}
	sm.storageMgr.UnpinFile(key)
}

func (sm *ShardManager) reinjectCustodial(key, payloadCIDStr string) bool {
	manifestCID, err := cid.Decode(key)
	if err != nil || sm.ps == nil {
		return false
	}
	currentShard := sm.getCurrentShard()
	nominal, err := common.TargetShardForPayload(payloadCIDStr, max(1, len(currentShard)))
	if err != nil {
		return false
	}
	target := sm.ResolveTargetShardForCustodial(nominal, payloadCIDStr)
	if target == currentShard {
		return false
	}
	if !sm.JoinShardAsObserver(target) {
		return false
	}
	defer sm.LeaveShardAsObserver(target)
	if err := sm.clusterMgr.JoinShard(sm.ctx, target); err != nil {
		slog.Error("custodial retry: failed to join target cluster", "target_shard", target, "error", err)
		return false
	}
	if err := sm.clusterMgr.Pin(sm.ctx, target, manifestCID, -1, -1); err != nil {
		slog.Error("custodial retry: failed to pin to target shard", "target_shard", target, "error", err)
		return false
	}
	if sm.signer != nil {
		sm.publishIngest(manifestCID, target)
	}
	slog.Info("custodial retry: re-injected file", "target_shard", target, "manifest", key)
	return true
}

func (sm *ShardManager) runCustodialRetryLoop() {
	sm.runEvery(custodialRetryInterval, sm.RunCustodialRetryPass)
}
