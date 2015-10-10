package raftpb

import "io"

type SnapshotReader struct {
	io.Reader
}

func (m *SnapshotReader) Marshal() (data []byte, err error) {
	return nil, nil
}

func (m *SnapshotReader) MarshalTo(data []byte) (int, error) {
	return 0, nil
}

func (m *SnapshotReader) Size() (n int) { return 0 }

func (m *SnapshotReader) Unmarshal(data []byte) error {
	return nil
}
