package memtable

import (
	"testing"
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
