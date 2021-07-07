package memtable

import (
	"testing"
	"unsafe"
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

func TestLeaseList_SameHash(t *testing.T) {
	var l leaseList
	l.init(4, 4000)

	id, ok := l.getLease(1234, 1000)
	assertTrue(t, ok)
	assertEqualUint32(t, 1, id)

	id, ok = l.getLease(1234, 2000)
	assertFalse(t, ok)
	assertEqualUint32(t, 0, id)
}

func TestLeaseList_SameHash_ReachExpireTime(t *testing.T) {
	var l leaseList
	l.init(4, 4000)

	id, ok := l.getLease(1234, 1000)
	assertTrue(t, ok)
	assertEqualUint32(t, 1, id)

	id, ok = l.getLease(1234, 5000)
	assertTrue(t, ok)
	assertEqualUint32(t, 2, id)
}

func TestLeaseList_SecondLease(t *testing.T) {
	var l leaseList
	l.init(4, 4000)

	l.getLease(1234, 1000)
	id, ok := l.getLease(2200, 2000)

	assertTrue(t, ok)
	assertEqualUint32(t, 2, id)
}

func TestLeaseList_WithHashZero(t *testing.T) {
	var l leaseList
	l.init(4, 4000)

	id, ok := l.getLease(0, 1000)
	assertTrue(t, ok)
	assertEqualUint32(t, 1, id)

	id, ok = l.getLease(0, 2000)
	assertFalse(t, ok)
	assertEqualUint32(t, 0, id)
}

func TestLeaseList_Delete(t *testing.T) {
	var l leaseList
	l.init(4, 4000)

	l.getLease(100, 1000)
	l.getLease(200, 2000)
	l.getLease(300, 2000)

	deleted := l.deleteLease(200, 2)
	assertTrue(t, deleted)

	id, ok := l.getLease(200, 3000)
	assertTrue(t, ok)
	assertEqualUint32(t, 4, id)
}

func TestLeaseList_Delete_WithDifferentLease(t *testing.T) {
	var l leaseList
	l.init(4, 4000)

	l.getLease(100, 1000)
	l.getLease(200, 2000)
	l.getLease(300, 2000)

	deleted := l.deleteLease(200, 3)
	assertFalse(t, deleted)

	id, ok := l.getLease(200, 3000)
	assertFalse(t, ok)
	assertEqualUint32(t, 0, id)
}

func TestLeaseList_ForceDelete(t *testing.T) {
	var l leaseList
	l.init(4, 4000)

	l.getLease(100, 1000)
	l.getLease(200, 2000)
	l.getLease(300, 2000)

	l.forceDelete(200)

	id, ok := l.getLease(200, 3000)
	assertTrue(t, ok)
	assertEqualUint32(t, 4, id)
}

func TestLeaseList_GetLease_WhenFull(t *testing.T) {
	var l leaseList
	l.init(4, 4000)
	l.getLease(100, 1000)
	l.getLease(200, 1000)
	l.getLease(300, 1000)
	l.getLease(400, 1000)

	id, ok := l.getLease(500, 1000)
	assertTrue(t, ok)
	assertEqualUint32(t, 5, id)

	id, ok = l.getLease(200, 1000)
	assertFalse(t, ok)
	assertEqualUint32(t, 0, id)

	id, ok = l.getLease(100, 1000)
	assertTrue(t, ok)
	assertEqualUint32(t, 6, id)

	id, ok = l.getLease(200, 1000)
	assertTrue(t, ok)
	assertEqualUint32(t, 7, id)
}

func TestLeaseListSize(t *testing.T) {
	size := unsafe.Sizeof(leaseList{})
	if size != 64 {
		t.Error("must equal 64, actual:", size)
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
