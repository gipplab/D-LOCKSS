package schema

import (
	"bytes"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p/core/peer"
)

// cborKV is a key-value pair for ordered CBOR map serialization.
// Val must be int64, string, []byte, or [][]cborKV (a list of maps).
type cborKV struct {
	Key string
	Val any
}

// marshalCBORMap encodes an ordered slice of key-value pairs as a CBOR map.
func marshalCBORMap(fields []cborKV) ([]byte, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	if err := assembleMap(nb, fields); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := dagcbor.Encode(nb.Build(), &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func assembleMap(na datamodel.NodeAssembler, fields []cborKV) error {
	ma, err := na.BeginMap(int64(len(fields)))
	if err != nil {
		return fmt.Errorf("begin map: %w", err)
	}
	for _, f := range fields {
		if err := ma.AssembleKey().AssignString(f.Key); err != nil {
			return err
		}
		switch v := f.Val.(type) {
		case int64:
			err = ma.AssembleValue().AssignInt(v)
		case string:
			err = ma.AssembleValue().AssignString(v)
		case []byte:
			err = ma.AssembleValue().AssignBytes(v)
		case [][]cborKV:
			err = assembleList(ma.AssembleValue(), v)
		default:
			return fmt.Errorf("unsupported CBOR value type %T for key %q", f.Val, f.Key)
		}
		if err != nil {
			return err
		}
	}
	return ma.Finish()
}

func assembleList(na datamodel.NodeAssembler, maps [][]cborKV) error {
	la, err := na.BeginList(int64(len(maps)))
	if err != nil {
		return err
	}
	for _, m := range maps {
		if err := assembleMap(la.AssembleValue(), m); err != nil {
			return err
		}
	}
	return la.Finish()
}

// decodeCBORMap decodes CBOR bytes into an IPLD map node.
func decodeCBORMap(data []byte) (datamodel.Node, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

func readInt(node datamodel.Node, key string) (int64, error) {
	n, err := node.LookupByString(key)
	if err != nil {
		return 0, err
	}
	return n.AsInt()
}

func readString(node datamodel.Node, key string) (string, error) {
	n, err := node.LookupByString(key)
	if err != nil {
		return "", err
	}
	return n.AsString()
}

func readBytes(node datamodel.Node, key string) ([]byte, error) {
	n, err := node.LookupByString(key)
	if err != nil {
		return nil, err
	}
	return n.AsBytes()
}

func readCID(node datamodel.Node, key string) (cid.Cid, error) {
	s, err := readString(node, key)
	if err != nil {
		return cid.Undef, err
	}
	return cid.Decode(s)
}

func readPeerID(node datamodel.Node, key string) (peer.ID, error) {
	s, err := readString(node, key)
	if err != nil {
		return "", err
	}
	return peer.Decode(s)
}
