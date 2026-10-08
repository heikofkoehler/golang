package ttlstore

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStore_BasicCRUD(t *testing.T) {
	s := New(WithShards(8))
	defer s.Close()

	_ = s.Set("key1", "value1")
	val, ok := s.Get("key1")
	if !ok || val != "value1" {
		t.Fatalf("expected value1, got %v", val)
	}

	deleted, _ := s.Delete("key1")
	if !deleted {
		t.Errorf("expected delete to return true")
	}

	_, ok = s.Get("key1")
	if ok {
		t.Errorf("expected key to be gone")
	}
}

func TestStore_LazyTTLExpiration(t *testing.T) {
	s := New(WithShards(4), WithCleanupInterval(10*time.Second)) // Long reaper interval to verify lazy cleanup
	defer s.Close()

	_ = s.SetWithTTL("temp-key", "temp-val", 50*time.Millisecond)

	val, ok := s.Get("temp-key")
	if !ok || val != "temp-val" {
		t.Fatalf("expected immediate get to succeed")
	}

	// Wait past TTL
	time.Sleep(80 * time.Millisecond)

	// Lazy cleanup should trigger on Get
	_, ok = s.Get("temp-key")
	if ok {
		t.Errorf("expected key to have expired lazily")
	}
}

func TestStore_ActiveBackgroundReaper(t *testing.T) {
	// Short reaper interval
	s := New(WithShards(4), WithCleanupInterval(25*time.Millisecond))
	defer s.Close()

	_ = s.SetWithTTL("active-exp-1", "val1", 30*time.Millisecond)
	_ = s.SetWithTTL("active-exp-2", "val2", 30*time.Millisecond)

	if s.Size() != 2 {
		t.Fatalf("expected size 2, got %d", s.Size())
	}

	// Wait for background reaper to sweep shards
	time.Sleep(120 * time.Millisecond)

	// Physical size should drop to 0 via active sweep without calling Get
	if s.Size() != 0 {
		t.Errorf("expected background reaper to prune expired keys, remaining size: %d", s.Size())
	}
}

func TestStore_SetNX(t *testing.T) {
	s := New(WithShards(4))
	defer s.Close()

	ok, _ := s.SetNX("lock-key", "holder-1", 100*time.Millisecond)
	if !ok {
		t.Fatalf("expected first SetNX to succeed")
	}

	// Second SetNX should fail
	ok, _ = s.SetNX("lock-key", "holder-2", 100*time.Millisecond)
	if ok {
		t.Fatalf("expected second SetNX to fail")
	}

	// Wait for expiration
	time.Sleep(120 * time.Millisecond)

	// Third SetNX should succeed after expiration
	ok, _ = s.SetNX("lock-key", "holder-3", 100*time.Millisecond)
	if !ok {
		t.Fatalf("expected third SetNX to succeed after expiration")
	}
}

func TestStore_AtomicCAS_Contention(t *testing.T) {
	// Concurrency Trap: Race window between check and swap
	s := New(WithShards(16))
	defer s.Close()

	const key = "cas-counter"
	_ = s.Set(key, 0)

	const increments = 100
	var wg sync.WaitGroup
	wg.Add(increments)

	var casFailures atomic.Int64

	for i := 0; i < increments; i++ {
		go func() {
			defer wg.Done()
			for {
				val, ok := s.Get(key)
				if !ok {
					t.Errorf("key vanished")
					return
				}
				cur := val.(int)
				swapped, err := s.CAS(key, cur, cur+1)
				if err != nil {
					t.Errorf("CAS error: %v", err)
					return
				}
				if swapped {
					break
				}
				casFailures.Add(1) // Contention detected and retried
			}
		}()
	}

	wg.Wait()

	finalVal, _ := s.Get(key)
	if finalVal.(int) != increments {
		t.Errorf("expected final counter %d, got %v", increments, finalVal)
	}

	// In a concurrent environment, CAS retries must have occurred
	if casFailures.Load() == 0 && increments > 10 {
		t.Logf("Note: 0 CAS failures occurred (scheduler was sequential)")
	}
}

func TestStore_CAD(t *testing.T) {
	s := New(WithShards(4))
	defer s.Close()

	_ = s.Set("cad-key", "secret-token")

	// Attempt CAD with wrong value
	deleted, _ := s.CAD("cad-key", "wrong-token")
	if deleted {
		t.Errorf("CAD should fail with mismatched token")
	}

	// Attempt CAD with matching value
	deleted, _ = s.CAD("cad-key", "secret-token")
	if !deleted {
		t.Errorf("CAD should succeed with matching token")
	}

	_, exists := s.Get("cad-key")
	if exists {
		t.Errorf("key should be deleted after CAD")
	}
}

func TestStore_HeavyReadWriteContention_RaceDetector(t *testing.T) {
	// Stress test with active readers, writers, deleters, and background reaper
	s := New(WithShards(16), WithCleanupInterval(15*time.Millisecond))
	defer s.Close()

	const numKeys = 20
	const workers = 40
	const opsPerWorker = 200

	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("k-%d", i%numKeys)
				switch (workerID + i) % 4 {
				case 0:
					_ = s.SetWithTTL(key, i, 50*time.Millisecond)
				case 1:
					_, _ = s.Get(key)
				case 2:
					_, _ = s.CAS(key, i-1, i)
				case 3:
					_, _ = s.Delete(key)
				}
			}
		}(w)
	}

	wg.Wait()
}
