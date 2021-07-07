package memtable

import (
	"github.com/cespare/xxhash"
	"github.com/coocood/freecache"
	"math/bits"
	"sync"
	"time"
)

type leaseEntry struct {
	hash      uint32
	lease     uint32
	createdAt uint32
}

type leaseList struct {
	mut       sync.Mutex
	list      []leaseEntry
	expire    uint32
	nextLease uint32
	_padding  [3]uint64 // to eliminate cache line false sharing
}

func (l *leaseList) init(size uint32, expire uint32) {
	l.list = make([]leaseEntry, size)
	l.expire = expire
}

func (l *leaseList) getLease(hash uint32, now uint32) (uint32, bool) {
	for i, e := range l.list {
		if e.createdAt+l.expire <= now {
			l.list[i] = leaseEntry{}
		}
	}

	minLease := l.list[0].lease
	minIndex := 0
	for i, e := range l.list {
		if e.hash == hash && e.lease > 0 {
			return 0, false
		}

		if e.lease < minLease {
			minLease = e.lease
			minIndex = i
		}
	}

	l.nextLease++
	l.list[minIndex] = leaseEntry{
		hash:      hash,
		lease:     l.nextLease,
		createdAt: now,
	}

	return l.nextLease, true
}

func (l *leaseList) deleteLease(hash uint32, lease uint32) bool {
	for i, e := range l.list {
		if e.hash == hash && e.lease == lease {
			l.list[i] = leaseEntry{}
			return true
		}
	}
	return false
}

func (l *leaseList) forceDelete(hash uint32) {
	for i, e := range l.list {
		if e.hash == hash {
			l.list[i] = leaseEntry{}
		}
	}
}

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

func computeHashKey(key []byte) uint32 {
	return uint32(hashFunc(key))
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
	hashKey := computeHashKey(key)
	index := hashKey % m.mask
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
