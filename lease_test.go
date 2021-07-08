package memtable

import (
	"math"
	"testing"
	"unsafe"
)

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

func TestLeaseList_GetLease_WhenFull_LeaseID_Check_Out_Of_Order(t *testing.T) {
	var l leaseList
	l.init(4, 4000)
	l.getLease(100, 1000)
	l.getLease(200, 1000)
	l.getLease(300, 1000)
	l.getLease(400, 1000)

	assertEqualUint32(t, 4, l.list[3].lease)

	affected := l.deleteLease(100, 1)
	assertTrue(t, affected)

	id, ok := l.getLease(500, 1000)
	assertTrue(t, ok)
	assertEqualUint32(t, 5, id)

	id, ok = l.getLease(600, 1000)
	assertTrue(t, ok)
	assertEqualUint32(t, 6, id)

	id, ok = l.getLease(300, 1200)
	assertFalse(t, ok)
}

func TestLeaseList_SameHash_When_NextLease_Equal_Max(t *testing.T) {
	var l leaseList
	l.init(4, 4000)
	l.nextLease = math.MaxUint32

	id, ok := l.getLease(1234, 1000)
	assertTrue(t, ok)
	assertEqualUint32(t, 1, id)

	id, ok = l.getLease(1234, 2000)
	assertFalse(t, ok)
	assertEqualUint32(t, 0, id)
}

func TestLeaseListSize(t *testing.T) {
	size := unsafe.Sizeof(leaseList{})
	if size != 64 {
		t.Error("must equal 64, actual:", size)
	}
}
