package shard

import (
	"context"
	"log/slog"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/pinset"
	"dlockss/internal/protocol"
	"dlockss/internal/schema"
)

func (m *Manager) publishText(shardID, line string) {
	if err := m.subs.Publish(m.ctx, shardID, []byte(line)); err != nil {
		slog.Debug("publish failed", "shard", shardID, "err", err)
	}
}

func (m *Manager) publishSigned(shardID string, msg schema.Message) error {
	if err := m.signer.Sign(msg); err != nil {
		return err
	}
	data, err := msg.MarshalCBOR()
	if err != nil {
		return err
	}
	return m.subs.Publish(m.ctx, shardID, data)
}

// publishHeartbeat sends one HEARTBEAT followed by a batch of PINNED
// announcements (round-robin over the pinned set).
func (m *Manager) publishHeartbeat(shardID string) {
	m.publishText(shardID, protocol.FormatHeartbeat(m.self, m.store.PinnedCount(), m.ourRole(), m.nodeName))
	for i := 0; i < heartbeatPinnedBatch; i++ {
		k, ok := m.store.NextToAnnounce()
		if !ok {
			break
		}
		m.publishText(shardID, protocol.FormatPinned(k.String()))
	}
}

// AnnouncePinned publishes a PINNED line on the current shard immediately
// (e.g. right after a pinset sync, so monitors see the replica before the
// next heartbeat).
func (m *Manager) AnnouncePinned(k cid.Cid) {
	m.publishText(m.CurrentShard(), protocol.FormatPinned(k.String()))
}

// sendIngestMessage publishes a signed IngestMessage addressed to shardID.
// With asObserver, the node silently joins the topic for the send.
func (m *Manager) sendIngestMessage(shardID string, manifest cid.Cid, asObserver bool) error {
	if asObserver && !m.subs.IsSubscribed(shardID) {
		if err := m.subs.JoinObserver(m.ctx, shardID); err != nil {
			return err
		}
		defer m.subs.LeaveObserver(shardID)
	}
	msg := &schema.IngestMessage{ShardID: shardID}
	msg.Env.Type = schema.MessageTypeIngest
	msg.Env.ManifestCID = manifest
	return m.publishSigned(shardID, msg)
}

// sendReplicationRequest publishes a signed ReplicationRequest to shardID,
// optionally as a silent observer.
func (m *Manager) sendReplicationRequest(shardID string, manifest cid.Cid, asObserver bool) error {
	if asObserver && !m.subs.IsSubscribed(shardID) {
		if err := m.subs.JoinObserver(m.ctx, shardID); err != nil {
			return err
		}
		defer m.subs.LeaveObserver(shardID)
	}
	msg := &schema.ReplicationRequest{}
	msg.Env.Type = schema.MessageTypeReplicationRequest
	msg.Env.ManifestCID = manifest
	return m.publishSigned(shardID, msg)
}

// AnnounceResponsibleFile handles a locally ingested file this node is
// responsible for: shard pinset, IngestMessage on the current shard (and to
// the payload's child when one is already populated), and DHT provides for
// both CIDs so public gateways can resolve them.
func (m *Manager) AnnounceResponsibleFile(manifest, payload cid.Cid) {
	current := m.CurrentShard()
	if _, err := m.pins.AddIfAbsent(current, manifest); err != nil {
		slog.Warn("ingest: pinset add failed", "cid", manifest, "err", err)
	}
	m.store.AddKnown(manifest)
	if err := m.sendIngestMessage(current, manifest, false); err != nil {
		slog.Warn("ingest: announce failed", "cid", manifest, "err", err)
	}

	// If a child shard already exists, tell it too: a split may be underway.
	child := TargetShardForPayload(payload.String(), len(current)+1)
	if m.probeShard(child, ingestChildProbeTimeout) >= 1 {
		if err := m.sendIngestMessage(child, manifest, true); err != nil {
			slog.Debug("ingest: child announce failed", "shard", child, "err", err)
		}
	}

	for _, k := range []cid.Cid{manifest, payload} {
		k := k
		m.track(func() {
			ctx, cancel := context.WithTimeout(m.ctx, dhtProvideTimeout)
			defer cancel()
			m.store.Provide(ctx, k)
		})
	}
}

// AnnounceCustodialFile handles a locally ingested file this node is NOT
// responsible for: announce it on the payload's nominal shard topic as a
// silent observer. Responsible receivers add it to their pin set; if the
// shard is momentarily empty, the placement pass re-nudges it every pass
// until enough providers exist, then releases the local copy. No DHT
// provide: the file's home is the target shard.
func (m *Manager) AnnounceCustodialFile(manifest, payload cid.Cid) {
	target := TargetShardForPayload(payload.String(), max(1, len(m.CurrentShard())))
	if target == m.CurrentShard() {
		m.AnnounceResponsibleFile(manifest, payload)
		return
	}
	m.store.AddKnown(manifest)
	if err := m.sendIngestMessage(target, manifest, true); err != nil {
		slog.Warn("custodial: announce failed", "cid", manifest, "err", err)
		return
	}
	slog.Info("custodial: announced file to its home shard", "cid", manifest, "shard", target)
}

// handoffConfirmed reports whether enough other nodes provide the key on the
// DHT for this node to release its custodial copy.
func (m *Manager) handoffConfirmed(k cid.Cid) bool {
	ctx, cancel := context.WithTimeout(m.ctx, handoffConfirmTimeout)
	defer cancel()
	need := m.cfg.MinPeersPerShard
	return m.store.ProviderCount(ctx, k, m.self, need) >= need
}

// unpinEverywhere removes the manifest from every joined pin set, from IPFS
// (manifest and payload), and from the local registry. Removing it from ALL
// pin sets matters: a membership left over in a pinset joined for custodial
// injection would otherwise re-pin the manifest after release, and the node
// would keep advertising a copy it no longer holds — corrupting handoff
// confirmation for every other custodian.
func (m *Manager) unpinEverywhere(manifest cid.Cid) {
	m.pins.RemoveFromAll(manifest)
	ctx, cancel := context.WithTimeout(m.ctx, manifestReadTimeout)
	payload := cid.Undef
	if ro, err := pinset.ReadManifest(ctx, m.client, manifest); err == nil {
		payload = ro.Payload
	}
	cancel()

	uctx, ucancel := context.WithTimeout(m.ctx, time.Minute)
	defer ucancel()
	if err := m.client.Unpin(uctx, manifest); err != nil {
		slog.Warn("unpin manifest failed", "cid", manifest, "err", err)
	}
	if payload.Defined() && payload != manifest {
		if err := m.client.Unpin(uctx, payload); err != nil {
			slog.Warn("unpin payload failed", "cid", payload, "err", err)
		}
	}
	m.store.Unpin(manifest)
}
