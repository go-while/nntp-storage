package storage

import (
	"log"
	"sync"
)

var (
	StorageCounter SC
)

/*
 *  storage.StorageCounter.Get("head_readcache_worker")
 *  storage.StorageCounter.Get("head_readcache_worker_max")
 *  storage.StorageCounter.Get("body_readcache_worker")
 *  storage.StorageCounter.Get("body_readcache_worker_max")
 *
 *  storage.StorageCounter.Get("head_writecache_worker")
 *  storage.StorageCounter.Get("head_writecache_worker_max")
 *  storage.StorageCounter.Get("body_writecache_worker")
 *  storage.StorageCounter.Get("body_writecache_worker_max")
 *
 *  storage.StorageCounter.Get("xref_link_worker")
 *  storage.StorageCounter.Get("redis_link_worker")
 *
 *  storage.StorageCounter.Get("head_cache_total_readb")
 *  storage.StorageCounter.Get("body_cache_total_readb")
 *
 *  storage.StorageCounter.Get("head_cache_total_reads")
 *  storage.StorageCounter.Get("body_cache_total_reads")
 *
 *  storage.StorageCounter.Get("head_cache_total_stats")
 *  storage.StorageCounter.Get("body_cache_total_stats")
 *
 *  storage.StorageCounter.Get("head_cache_total_redis")
 *  storage.StorageCounter.Get("body_cache_total_redis")
 *
 *  storage.StorageCounter.Get("head_cache_total_wrote_bytes")
 *  storage.StorageCounter.Get("body_cache_total_wrote_bytes")
 *
 *  storage.StorageCounter.Get("head_cache_total_writes")
 *  storage.StorageCounter.Get("body_cache_total_writes")
 *
 */

// StorageCounter is safe to use concurrently.
type SC struct {
	v       map[string]uint64
	MapSize int
	mux     sync.Mutex
} // end SC struct

func (sc *SC) Make_SC() {
	sc.mux.Lock()
	if sc.v == nil {
		sc.MapSize = 16
		sc.v = make(map[string]uint64, sc.MapSize)

	}
	sc.mux.Unlock()
} // end func SC.Make

func (sc *SC) Del(countername string) {
	sc.mux.Lock()
	delete(sc.v, countername)
	sc.mux.Unlock()
	sc.gc_SC()
} // end func SC.Del

func (sc *SC) Inc(countername string) {
	sc.Checkmap()
	sc.mux.Lock()
	sc.v[countername] += 1
	sc.mux.Unlock()
} // end func SC.Inc

func (sc *SC) Dec(countername string) {
	sc.Checkmap()
	sc.mux.Lock()
	if sc.v[countername] > 0 {
		sc.v[countername] -= 1
	} else {
		log.Printf("ERROR SC.Dec is 0 countername='%s'", countername)
	}
	sc.mux.Unlock()
} // end func SC.Dec

func (sc *SC) Add(countername string, value uint64) {
	if value <= 0 {
		return
	}
	sc.Checkmap()
	sc.mux.Lock()
	sc.v[countername] += value
	sc.mux.Unlock()
} // end func SC.Add

func (sc *SC) Sub(countername string, value uint64) {
	sc.Checkmap()
	sc.mux.Lock()
	newval := sc.v[countername] - value
	if newval >= 0 {
		sc.v[countername] = newval
	} else {
		log.Printf("WARN SC.Sub countername='%s' value=%d newval=%d < 0", countername, value, newval)
		sc.v[countername] = 0
	}
	sc.mux.Unlock()
} // end func SC.Sub

func (sc *SC) Get(countername string) uint64 {
	sc.Checkmap()
	sc.mux.Lock()
	retval := uint64(sc.v[countername])
	sc.mux.Unlock()
	return retval
} // end func SC.Get

func (sc *SC) Set(countername string, value uint64) {
	sc.Checkmap()
	if value >= 0 {
		sc.mux.Lock()
		sc.v[countername] = value
		sc.mux.Unlock()
	}
} // end func SC.Set

func (sc *SC) Init(countername string, value uint64) {
	sc.Checkmap()
	sc.Set(countername, value)
} // end func SC.Init

func (sc *SC) Checkmap() {
	sc.mux.Lock()
	if len(sc.v) == sc.MapSize {
		log.Printf("SC.checkmap need grow len(c.v)=%d c.MapSize=%d", len(sc.v), sc.MapSize)
		//map is full, copy and grow map
		newMapSize := sc.MapSize + 8
		newMap := make(map[string]uint64, newMapSize)
		for key, val := range sc.v {
			newMap[string(key)] = uint64(val)
		}
		sc.v = nil
		sc.v = newMap
		sc.MapSize = newMapSize
		log.Printf("SC.checkmap grown len(c.v)=%d c.MapSize=%d", len(sc.v), sc.MapSize)
	}
	sc.mux.Unlock()
} // end func SC.Checkmap

func (sc *SC) gc_SC() {
	sc.mux.Lock()                  // check if we can shrink the map
	if len(sc.v) < sc.MapSize-16 { // if map contains less than max mapsize-16 elements
		newMapSize := len(sc.v) + 8 // calculate new mapsize, give 8 space
		log.Printf("StorageCounter gc_SC: len(sc.v)=%d sc.MapSize=%d newMapSize=%d", len(sc.v), sc.MapSize, newMapSize)
		newMap := make(map[string]uint64, newMapSize)
		for key, val := range sc.v {
			newMap[string(key)] = uint64(val)
		}
		sc.v = nil
		sc.v = newMap
		sc.MapSize = newMapSize
	}
	sc.mux.Unlock()
} // end func gc_SC
