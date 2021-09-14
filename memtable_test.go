package memtable

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func assertTrue(t *testing.T, b bool) {
	t.Helper()
	if !b {
		t.Error("Must be true")
	}
}

func assertFalse(t *testing.T, b bool) {
	t.Helper()
	if b {
		t.Error("Must be false")
	}
}

func assertEqualUint32(t *testing.T, expected, value uint32) {
	t.Helper()
	if value != expected {
		t.Errorf("Must be equal, expected: %v, actual: %v", expected, value)
	}
}

func assertEqualBytes(t *testing.T, expected, actual []byte) {
	t.Helper()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Must be equal, expected: %v, actual: %v", expected, actual)
	}
}

func assertEqualGetStatus(t *testing.T, expected, actual GetStatus) {
	t.Helper()
	if actual != expected {
		t.Errorf("Must be equal, expected: %v, actual: %v", expected, actual)
	}
}

func TestCeilPowerOfTwo(t *testing.T) {
	result := ceilPowerOfTwo(100)
	assertEqualUint32(t, 128, result)

	result = ceilPowerOfTwo(16)
	assertEqualUint32(t, 16, result)

	result = ceilPowerOfTwo(255)
	assertEqualUint32(t, 256, result)

	result = ceilPowerOfTwo(0)
	assertEqualUint32(t, 1, result)
}

func TestMemtable_New(t *testing.T) {
	m := New(1<<20, WithNumBuckets(120), WithLeaseListSize(5))
	if m.mask != 0x7f {
		t.Error("expected 0x7f, actual:", m.mask)
	}
	if len(m.leases) != 128 {
		t.Error("expected 128, actual:", len(m.leases))
	}
	if len(m.leases[0].list) != 8 {
		t.Error("expected 8, actual:", len(m.leases[0].list))
	}
}

func TestMemtable_Get_Rejected(t *testing.T) {
	m := New(1 << 20)
	key1 := []byte("key1")

	result := m.Get(key1)

	assertEqualBytes(t, nil, result.Value)
	assertEqualUint32(t, 1, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)

	result = m.Get(key1)
	assertEqualBytes(t, nil, result.Value)
	assertEqualUint32(t, 0, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseRejected, result.Status)
}

func TestMemtable_Set_OK(t *testing.T) {
	m := New(1 << 20)
	key1 := []byte("key1")

	result := m.Get(key1)

	assertEqualBytes(t, nil, result.Value)
	assertEqualUint32(t, 1, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)

	affected := m.Set(key1, result.LeaseID, []byte("value1"))
	assertTrue(t, affected)

	result = m.Get(key1)
	assertEqualBytes(t, []byte("value1"), result.Value)
	assertEqualUint32(t, 0, result.LeaseID)
	assertEqualGetStatus(t, GetStatusFound, result.Status)
}

func TestMemtable_Set_Not_Affected_After_Invalidate(t *testing.T) {
	m := New(1 << 20)
	key1 := []byte("key1")

	result := m.Get(key1)

	affected := m.Invalidate(key1)
	assertFalse(t, affected)

	affected = m.Set(key1, result.LeaseID, []byte("value1"))
	assertFalse(t, affected)

	result = m.Get(key1)
	assertEqualBytes(t, nil, result.Value)
	assertEqualUint32(t, 2, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)
}

func TestMemtable_Invalidate_Affected(t *testing.T) {
	m := New(1 << 20)
	key1 := []byte("key1")

	result := m.Get(key1)

	affected := m.Set(key1, result.LeaseID, []byte("value1"))
	assertTrue(t, affected)

	affected = m.Invalidate(key1)
	assertTrue(t, affected)

	result = m.Get(key1)
	assertEqualBytes(t, nil, result.Value)
	assertEqualUint32(t, 2, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)
}

func TestMemtable_Double_Set_Not_OK(t *testing.T) {
	m := New(1 << 20)
	key1 := []byte("key1")

	result := m.Get(key1)

	assertEqualBytes(t, nil, result.Value)
	assertEqualUint32(t, 1, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)

	affected := m.Set(key1, result.LeaseID, []byte("value1"))
	assertTrue(t, affected)

	affected = m.Set(key1, result.LeaseID, []byte("value2"))
	assertFalse(t, affected)

	result = m.Get(key1)
	assertEqualBytes(t, []byte("value1"), result.Value)
	assertEqualUint32(t, 0, result.LeaseID)
	assertEqualGetStatus(t, GetStatusFound, result.Status)
}

func TestMemtable_Get_Second_Times_After_Lease_Timeout(t *testing.T) {
	m := New(1<<20, WithLeaseTimeout(2))

	key := []byte("key")
	result := m.Get(key)
	assertEqualBytes(t, nil, result.Value)
	assertEqualUint32(t, 1, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)

	time.Sleep(3 * time.Second)

	result = m.Get(key)
	assertEqualBytes(t, nil, result.Value)
	assertEqualUint32(t, 2, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)
}

func TestMemtable_Get_Second_Times_Before_Lease_Timeout(t *testing.T) {
	m := New(1<<20, WithLeaseTimeout(2))

	key := []byte("key")
	result := m.Get(key)
	assertEqualBytes(t, nil, result.Value)
	assertEqualUint32(t, 1, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)

	time.Sleep(1 * time.Second)

	result = m.Get(key)
	assertEqualBytes(t, nil, result.Value)
	assertEqualUint32(t, 0, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseRejected, result.Status)
}

func TestComputeHashAndIndex(t *testing.T) {
	hash := uint64(0xaabbccdd11223344)
	key, index := computeHashKeyAndIndex(hash, 0xff)
	if key != 0xaabbccdd {
		t.Error("expected 0xaabbccdd, actual:", key)
	}
	if index != 0x44 {
		t.Error("expected 0x44, actual:", index)
	}
}

func BenchmarkGetSet(b *testing.B) {
	b.StopTimer()

	m := New(128 << 20)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprint("key-", i))
		result := m.Get(key)
		affected := m.Set(key, result.LeaseID, []byte("value"))
		if !affected {
			panic("not affected")
		}
	}
}

func BenchmarkParallelGetSet(b *testing.B) {
	b.StopTimer()

	m := New(128<<20, WithNumBuckets(128), WithLeaseListSize(4))

	b.StartTimer()

	index := uint64(0)
	b.RunParallel(func(pb *testing.PB) {
		noopCount := 0

		for pb.Next() {
			i := atomic.AddUint64(&index, 1)

			key := []byte(fmt.Sprint("key-", i))
			result := m.Get(key)
			affected := m.Set(key, result.LeaseID, []byte("value"))
			if !affected {
				noopCount++
			}
		}

		fmt.Println(noopCount)
	})
}
