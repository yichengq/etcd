package main

import (
	"encoding/binary"
	"fmt"
	"flag"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

const msgApp int64 = 3

const recordInterval = 10 * time.Second

var (
	mode  = flag.String("mode", "both", "")
	port  = flag.Int("port", 8001, "port to listen for followers")
	peers = flag.String("peers", "http://127.0.0.1:8001", "host of followers")
)

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	println("run in mode", *mode)
	if *mode == "both" || *mode == "follower" {
		go http.ListenAndServe(fmt.Sprintf(":%d", *port), &followerServer{size: 128})
	}
	if *mode == "both" || *mode == "leader" {
		go func() {
			l := NewLeaderServer(strings.Split(*peers, ",")...)
			// wait for leader to build connection with followers
			time.Sleep(time.Second)
			l.KeepStreaming(128)
		}()
	}
	for {
		time.Sleep(time.Minute)
	}
}

type buffer struct {
	dst io.Writer
}

func (b *buffer) WriteTo(dst io.Writer) (n int64, err error) {
	b.dst = dst
	time.Sleep(time.Hour)
	return 0, nil
}

func (b *buffer) Read(p []byte) (n int, err error) {
	panic("unimplemented")
	return 0, nil
}

func (b *buffer) Close() error { return nil }

type leaderServer struct {
	bufs []buffer
}

func NewLeaderServer(followerURLs ...string) *leaderServer {
	s := &leaderServer{}
	s.startSender(followerURLs)
	return s
}

func (s *leaderServer) startSender(urls []string) {
	s.bufs = make([]buffer, len(urls))
	for i, url := range urls {
		req, err := http.NewRequest("PUT", url, &s.bufs[i])
		if err != nil {
			log.Fatal(err)
		}
		req.ContentLength = -1
		go func() {
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Println(err)
			}
			log.Println("do request %v finish", req)
			resp.Body.Close()
		}()
	}
}

const bufferSize = 1000

func (s *leaderServer) KeepStreaming(size int) {
	tick := time.NewTicker(recordInterval)
	start := time.Now()
	var li, i int64
	var encodetime time.Duration
	c := make(chan []byte, 50)
	go s.streamWrite(c)
	for {
		stt := time.Now()
		wbs := make([]byte, bufferSize*(size+100))
		offset := 0
		for j := 0; j < bufferSize; j++ {
			m := Message{Type: msgApp, Entries: []Entry{{Index: i, Data: make([]byte, size)}}}
			ent := m.Entries[0]
			l := binary.PutVarint(wbs[offset:], int64(ent.Size()))
			offset += l
			n, err := ent.MarshalTo(wbs[offset:])
			if err != nil {
				log.Fatal(err)
			}
			offset += n
			i++
		}
		wbs = wbs[:offset]
		edt := time.Now()
		encodetime += edt.Sub(stt)
		c <- wbs
		select {
		case <-tick.C:
			diff := i - li
			log.Printf("encode %v(real data: %v) entries in all after %v(encodetime: %v)", diff, ByteSize(diff*int64(size)), time.Since(start), encodetime)
			start = time.Now()
			li = i
			encodetime = 0
		default:
		}
	}
}

func (s *leaderServer) streamWrite(c chan []byte) {
	tick := time.NewTicker(recordInterval)
	start := time.Now()
	var size ByteSize
	var nwwrite time.Duration
	for {
		wbs := <-c
		stt := time.Now()
		for _, buf := range s.bufs {
			if _, err := buf.dst.Write(wbs); err != nil {
				log.Fatal(err)
			}
		}
		nwwrite += time.Since(stt)
		size += ByteSize(len(wbs))
		select {
		case <-tick.C:
			log.Printf("write out %v in all after %v(nwwrite: %v)", size, time.Since(start), nwwrite)
			start = time.Now()
			size = 0
			nwwrite = 0
		default:
		}
	}
}

type followerServer struct {
	size int
}

func (s *followerServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.size == 0 {
		panic("set size please")
	}
	s.HandleStream(w, r)
}

func (s *followerServer) reservedSpace() int {
	return s.size + 100
}

func (s *followerServer) HandleStream(w http.ResponseWriter, r *http.Request) {
	bufsize := 102400000
	tick := time.NewTicker(recordInterval)
	start := time.Now()
	var nwread time.Duration
	var size ByteSize
	c := make(chan []byte, 50)
	go s.streamDecode(c)
	for {
		buf := make([]byte, bufsize)
		stt := time.Now()
		l, err := r.Body.Read(buf[s.reservedSpace():])
		if err != nil {
			log.Fatalf("body read err = %v when get length %d", err, l)
		}
		nwread += time.Since(stt)
		buf = buf[:s.reservedSpace()+l]
		size += ByteSize(l)
		c <- buf
		select {
		case <-tick.C:
			log.Printf("read in %v after %v(nwread: %v)", size, time.Since(start), nwread)
			start = time.Now()
			size = 0
			nwread = 0
		default:
		}
	}
}

func (s *followerServer) streamDecode(c chan []byte) {
	tick := time.NewTicker(recordInterval)
	start := time.Now()
	var decodetime time.Duration
	var li, i int64
	var left []byte
	for {
		buf := <-c
		stt := time.Now()
		copy(buf[s.reservedSpace()-len(left):], left)
		buf = buf[s.reservedSpace()-len(left):]
		offset := 0
		l := len(buf)
		var m Message
		for l > offset+s.size+100 {
			x, n := binary.Varint(buf[offset:])
			offset += n
			ll := int(x)
			var ent Entry
			if err := ent.Unmarshal(buf[offset : offset+ll]); err != nil {
				log.Fatalf("Unmarshal %v (len = %d) error: %v", buf[offset:offset+ll], ll, err)
			}
			if ent.Index != i {
				log.Fatalf("index = %d, want %d", ent.Index, i)
			}
			m.Entries = append(m.Entries, ent)
			i++
			offset += ll
		}
		left = buf[offset:l]
		decodetime += time.Since(stt)
		select {
		case <-tick.C:
			diff := i - li
			log.Printf("decode %v(real data: %v) entries after %v(decodetime: %v)", diff, ByteSize(diff*int64(s.size)), time.Since(start), decodetime)
			start = time.Now()
			li = i
			decodetime = 0
		default:
		}
	}
}
