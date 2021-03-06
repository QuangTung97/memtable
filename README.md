# Library for Consistent Cache
[![Go Report Card](https://goreportcard.com/badge/github.com/quangtung97/memtable)](https://goreportcard.com/report/github.com/quangtung97/memtable)
[![Coverage Status](https://coveralls.io/repos/github/QuangTung97/memtable/badge.svg?branch=master)](https://coveralls.io/github/QuangTung97/memtable?branch=master)

Based on the [freecache](https://github.com/coocood/freecache) library and the 'leasing' mechanism of the paper
[Scaling Memcache at Facebook](https://www.usenix.org/system/files/conference/nsdi13/nsdi13-final170_update.pdf).

## Usage

```go
cacheSize := 100 * 1024 * 1024 // 100MB
cache := memtable.New(cacheSize)

for {
    key := []byte("some-key")
    result := cache.Get(key)
    if result.Status == memtable.GetStatusLeaseRejected {
    	time.Sleep(100 * time.Millisecond)
    	continue
    }
    
    if result.Status == memtable.GetStatusFound {
    	// cache hit
    	fmt.Println("Got value:", result.Value)
    	return
    }
    
    // cache miss but lease is granted
    // get data from database
    value := []byte("some-value")
    
    affected := cache.Set(key, result.LeaseID, value)
    fmt.Println("Affected:", affected)
    return
}
```

## License

The MIT License