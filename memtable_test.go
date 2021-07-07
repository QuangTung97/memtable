package memtable

import (
	"reflect"
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

func TestMemtable_Set_Not_Affected_After_Invalidate_Affected(t *testing.T) {
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
