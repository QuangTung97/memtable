package memtable

import (
	"github.com/cespare/xxhash"
	"github.com/coocood/freecache"
	"math/bits"
	"time"
)

// Memtable ...
type Memtable struct {
	leases []leaseList
	mask   uint32
	cache  *freecache.Cache
}

// New ...
func New(memsize int, options ...Option) *Memtable {
	opts := computeOptions(options...)

	leases := make([]leaseList, opts.numBuckets)
	for i := range leases {
		leases[i].init(opts.entryListSize, opts.leaseTimeout)
	}

	return &Memtable{
		leases: leases,
		cache:  freecache.NewCache(memsize),
		mask:   opts.numBuckets - 1,
	}
}

func hashFunc(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func getNow() uint32 {
	return uint32(time.Now().Unix())
}

// GetStatus for cache Get
type GetStatus int

const (
	// GetStatusFound for normal cache hit case
	GetStatusFound GetStatus = iota
	// GetStatusLeaseGranted when cache miss and lease is granted
	GetStatusLeaseGranted
	// GetStatusLeaseRejected when cache miss and lease is not granted
	GetStatusLeaseRejected
)

// GetResult for result when calling Get
type GetResult struct {
	Value   []byte
	LeaseID uint32
	Status  GetStatus
}

func (m *Memtable) getLeaseList(key []byte) (uint32, *leaseList) {
	hash := hashFunc(key)
	index := uint32(hash) % m.mask
	hashKey := uint32(hash >> 32)
	return hashKey, &m.leases[index]
}

// Get value from the cache
func (m *Memtable) Get(key []byte) GetResult {
	hashKey, l := m.getLeaseList(key)

	l.mut.Lock()
	defer l.mut.Unlock()

	value, err := m.cache.Get(key)
	if err == freecache.ErrNotFound {
		leaseID, ok := l.getLease(hashKey, getNow())
		if !ok {
			return GetResult{
				Status: GetStatusLeaseRejected,
			}
		}

		return GetResult{
			LeaseID: leaseID,
			Status:  GetStatusLeaseGranted,
		}
	}

	return GetResult{
		Value:  value,
		Status: GetStatusFound,
	}
}

// Set value to the cache
func (m *Memtable) Set(key []byte, leaseID uint32, value []byte) (affected bool) {
	hashKey, l := m.getLeaseList(key)

	l.mut.Lock()
	defer l.mut.Unlock()

	deleted := l.deleteLease(hashKey, leaseID)
	if !deleted {
		return false
	}

	err := m.cache.Set(key, value, 0)
	if err != nil {
		return false
	}
	return true
}

// Invalidate an entry from the cache
func (m *Memtable) Invalidate(key []byte) (affected bool) {
	hashKey, l := m.getLeaseList(key)

	l.mut.Lock()
	defer l.mut.Unlock()

	l.forceDelete(hashKey)

	return m.cache.Del(key)
}

func ceilPowerOfTwo(n uint32) uint32 {
	if n == 0 {
		return 1
	}
	shift := 32 - bits.LeadingZeros32(n-1)
	return 1 << shift
}
