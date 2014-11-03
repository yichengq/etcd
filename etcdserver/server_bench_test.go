package etcdserver

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/Godeps/_workspace/src/code.google.com/p/go.net/context"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/store"
)

const (
	// bytes per second (=700Mb/s)
	networkSpeed uint64 = 700 * 1024 * 1024 / 8
	// ops per second (=370op/s)
	diskSpeed uint64 = 370

	networkByteTime time.Duration = time.Duration(uint64(1e9) / networkSpeed) * time.Nanosecond
	diskOpTime time.Duration = time.Duration(uint64(1e9) / diskSpeed) * time.Nanosecond

	httpHeaderLen = 128
)

func BenchmarkSimulatedClusterOf3Write(b *testing.B) { benchmarkSimulatedClusterWrite(b, 3) }

func benchmarkSimulatedClusterWrite(b *testing.B, ns uint64) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var alld time.Duration

	ss := make([]*EtcdServer, ns)

	ids := make([]uint64, ns)
	for i := uint64(0); i < ns; i++ {
		ids[i] = i + 1
	}
	members := mustMakePeerSlice(nil, ids...)
	senders := make([]simulatedSender, ns)
	storages := make([]simulatedStorage, ns)
	for i := uint64(0); i < ns; i++ {
		id := i + 1
		n := raft.StartNode(id, members, 10, 1)
		tk := time.NewTicker(50 * time.Millisecond)
		defer tk.Stop()
		cl := newCluster("abc")
		cl.SetStore(&storeRecorder{})
		senders[i].srvs = ss
		srv := &EtcdServer{
			node:    n,
			store:   store.New(),
			send:    (&senders[i]).send,
			storage: &storages[i],
			Ticker:  tk.C,
			Cluster: cl,
		}
		srv.start()
		ss[i] = srv
	}

	reqs := make(chan bool, 1000)
	val := string(make([]byte, 1024))
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ok := <-reqs; !ok {
					return
				}
				r := pb.Request{
					Method: "PUT",
					ID:     GenID(),
					Path:   "/foo",
					Val:    val,
				}
				j := rand.Intn(len(ss))
				if _, err := ss[j].Do(ctx, r); err != nil {
					b.Fatal(err)
				}
			}
		}()
	}

	b.SetBytes(1024)
	// wait for goroutines to start running
	time.Sleep(time.Second)
	start := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reqs <- true
	}
	close(reqs)
	wg.Wait()
	b.StopTimer()
	alld = time.Since(start)
	b.Logf("%d requests: all time %v, network time as leader %v, disk time as leader %v, the number of entries sent as leader %d, the number of entries written as leader %d", b.N, alld, senders[0].d, storages[0].d, senders[0].entnum, storages[0].entnum)

	for _, sv := range ss {
		sv.Stop()
	}
}

type simulatedStorage struct{
	d time.Duration
	entnum int
}

func (s *simulatedStorage) Save(st raftpb.HardState, ents []raftpb.Entry) {
	if raft.IsEmptyHardState(st) && len(ents) == 0 {
		return
	}
	s.d += diskOpTime
	s.entnum += len(ents)
	time.Sleep(diskOpTime)
}
func (s *simulatedStorage) SaveSnap(snap raftpb.Snapshot) {
	if raft.IsEmptySnap(snap) {
		return
	}
	panic("TODO")
}
func (s *simulatedStorage) Cut() error { panic("TODO") }

type simulatedSender struct{
	d time.Duration
	entnum int
	sync.Mutex
	srvs []*EtcdServer
}

func (s *simulatedSender) send(msgs []raftpb.Message) {
	go func() {
		s.Lock()
		defer s.Unlock()
		for _, m := range msgs {
			t := time.Duration(m.Size() + httpHeaderLen) * networkByteTime
			s.d += t
			s.entnum += len(m.Entries)
			time.Sleep(t)
			s.srvs[m.To-1].node.Step(context.TODO(), m)
		}
	}()
}
