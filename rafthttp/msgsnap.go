package rafthttp

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/storage"
)

type msgSnapEncoder struct {
	w       io.Writer
	v3store storage.KV
}

func (enc *msgSnapEncoder) encode(m raftpb.Message) error {
	if isLinkHeartbeatMessage(m) {
		return binary.Write(enc.w, binary.BigEndian, uint64(0))
	}

	if err := binary.Write(enc.w, binary.BigEndian, uint64(m.Size())); err != nil {
		return err
	}
	if _, err := enc.w.Write(pbutil.MustMarshal(&m)); err != nil {
		return err
	}

	_, err := enc.v3store.Snapshot(enc.w)
	return err
}

type msgSnapDecoder struct {
	r io.Reader
}

func (dec *msgSnapDecoder) decode() (raftpb.Message, error) {
	var m raftpb.Message
	var l uint64
	if err := binary.Read(dec.r, binary.BigEndian, &l); err != nil {
		return m, err
	}
	if l == 0 {
		return linkHeartbeatMessage, nil
	}

	buf := make([]byte, int(l))
	if _, err := io.ReadFull(dec.r, buf); err != nil {
		return m, err
	}
	if err := m.Unmarshal(buf); err != nil {
		return m, err
	}

	if err := binary.Read(dec.r, binary.BigEndian, &l); err != nil {
		return m, err
	}
	// TODO: what should do if the file exists
	// TODO: file name is tricky
	f, err := os.OpenFile(fmt.Sprintf("v3store_%016x", m.Snapshot.Metadata.Index), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return m, err
	}
	defer f.Close()
	_, err = io.CopyN(f, dec.r, int64(l))
	return m, err
}
