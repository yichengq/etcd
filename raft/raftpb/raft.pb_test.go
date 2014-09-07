package raftpb

import (
	"runtime"
	"testing"
)

func BenchmarkLogEntryMarshal_8b(b *testing.B)  { benchmarkLogEntryMarshal(b, 8) }
func BenchmarkLogEntryMarshal_32b(b *testing.B) { benchmarkLogEntryMarshal(b, 32) }

func benchmarkLogEntryMarshal(b *testing.B, sz int) {
	entry := &Entry{Data: make([]byte, sz)}

	data, err := entry.Marshal()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := entry.Marshal(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	runtime.GC()
}

func BenchmarkLogEntryMarshalTo_8b(b *testing.B)  { benchmarkLogEntryMarshalTo(b, 8) }
func BenchmarkLogEntryMarshalTo_32b(b *testing.B) { benchmarkLogEntryMarshalTo(b, 32) }

func benchmarkLogEntryMarshalTo(b *testing.B, sz int) {
	entry := &Entry{Data: make([]byte, sz)}
	data := make([]byte, entry.Size())

	b.SetBytes(int64(entry.Size()))
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := entry.MarshalTo(data); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	runtime.GC()
}

func BenchmarkLogEntryUnmarshal_8b(b *testing.B)  { benchmarkLogEntryUnmarshal(b, 8) }
func BenchmarkLogEntryUnmarshal_32b(b *testing.B) { benchmarkLogEntryUnmarshal(b, 32) }

func benchmarkLogEntryUnmarshal(b *testing.B, sz int) {
	entry := &Entry{Data: make([]byte, sz)}

	data, err := entry.Marshal()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	// Decode from the buffer.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var entry Entry
		if err := entry.Unmarshal(data); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	runtime.GC()
}
