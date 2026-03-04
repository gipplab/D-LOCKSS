package schema

// --- UnreplicateRequest CBOR ---

func (m *UnreplicateRequest) MarshalCBOR() ([]byte, error) {
	return marshalFields(&m.SignedEnvelope, m.specificFields(), true)
}

func (m *UnreplicateRequest) MarshalCBORForSigning() ([]byte, error) {
	return marshalFields(&m.SignedEnvelope, m.specificFields(), false)
}

func (m *UnreplicateRequest) specificFields() []cborKV {
	return []cborKV{
		{"excess_count", int64(m.ExcessCount)},
		{"current_count", int64(m.CurrentCount)},
	}
}

func (m *UnreplicateRequest) UnmarshalCBOR(data []byte) error {
	node, err := decodeCBORMap(data)
	if err != nil {
		return err
	}
	if err := m.unmarshalEnvelope(node); err != nil {
		return err
	}
	excessInt, err := readInt(node, "excess_count")
	if err != nil {
		return err
	}
	m.ExcessCount = int(excessInt)
	currentInt, err := readInt(node, "current_count")
	if err != nil {
		return err
	}
	m.CurrentCount = int(currentInt)
	return nil
}
