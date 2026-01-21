package schema

import (
	"bytes"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p/core/peer"
)

// MarshalCBOR serializes an UnreplicateRequest to CBOR format.
func (m *UnreplicateRequest) MarshalCBOR() ([]byte, error) {
	return m.marshalCBOR(true)
}

// MarshalCBORForSigning serializes the message without the signature field.
func (m *UnreplicateRequest) MarshalCBORForSigning() ([]byte, error) {
	return m.marshalCBOR(false)
}

func (m *UnreplicateRequest) marshalCBOR(includeSig bool) ([]byte, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	fieldCount := int64(7)
	if includeSig {
		fieldCount = 8
	}
	ma, err := nb.BeginMap(fieldCount)
	if err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("type"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignInt(int64(m.Type)); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("manifest_cid"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignString(m.ManifestCID.String()); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("excess_count"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignInt(int64(m.ExcessCount)); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("current_count"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignInt(int64(m.CurrentCount)); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("sender_id"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignString(m.SenderID.String()); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("ts"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignInt(m.Timestamp); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("nonce"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignBytes(m.Nonce); err != nil {
		return nil, err
	}

	if includeSig {
		if err := ma.AssembleKey().AssignString("sig"); err != nil {
			return nil, err
		}
		if err := ma.AssembleValue().AssignBytes(m.Sig); err != nil {
			return nil, err
		}
	}

	if err := ma.Finish(); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := dagcbor.Encode(nb.Build(), &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalCBOR deserializes an UnreplicateRequest from CBOR format.
func (m *UnreplicateRequest) UnmarshalCBOR(data []byte) error {
	nb := basicnode.Prototype.Map.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(data)); err != nil {
		return err
	}
	node := nb.Build()

	typeNode, err := node.LookupByString("type")
	if err != nil {
		return err
	}
	typeInt, err := typeNode.AsInt()
	if err != nil {
		return err
	}
	m.Type = MessageType(typeInt)

	manifestNode, err := node.LookupByString("manifest_cid")
	if err != nil {
		return err
	}
	manifestStr, err := manifestNode.AsString()
	if err != nil {
		return err
	}
	m.ManifestCID, err = cid.Decode(manifestStr)
	if err != nil {
		return err
	}

	excessNode, err := node.LookupByString("excess_count")
	if err != nil {
		return err
	}
	excessInt, err := excessNode.AsInt()
	if err != nil {
		return err
	}
	m.ExcessCount = int(excessInt)

	currentNode, err := node.LookupByString("current_count")
	if err != nil {
		return err
	}
	currentInt, err := currentNode.AsInt()
	if err != nil {
		return err
	}
	m.CurrentCount = int(currentInt)

	senderNode, err := node.LookupByString("sender_id")
	if err != nil {
		return err
	}
	senderStr, err := senderNode.AsString()
	if err != nil {
		return err
	}
	m.SenderID, err = peer.Decode(senderStr)
	if err != nil {
		return err
	}

	tsNode, err := node.LookupByString("ts")
	if err != nil {
		return err
	}
	m.Timestamp, err = tsNode.AsInt()
	if err != nil {
		return err
	}

	nonceNode, err := node.LookupByString("nonce")
	if err != nil {
		return err
	}
	m.Nonce, err = nonceNode.AsBytes()
	if err != nil {
		return err
	}

	sigNode, err := node.LookupByString("sig")
	if err != nil {
		return err
	}
	m.Sig, err = sigNode.AsBytes()
	if err != nil {
		return err
	}

	return nil
}
