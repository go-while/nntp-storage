package storage

import (
	"fmt"
	"github.com/go-while/go-utils"
	"github.com/gomodule/redigo/redis"
	"log"
	"os"
	"sync"
)

var (
	XrefLinker XL
)

/* XREF links:
 *  "cachedir/[head|body]/c1/c2/c3/messageidhash[:3].[head|body]"
 *   to cachedir/[head|body]/groups/grouphash/msgnum
 */
type XREF struct {
	Msgid     string
	Msgidhash string
	Group     string
	Grouphash string
	Msgnum    uint64
} // end XREF struct

type XL struct {
	mux             sync.Mutex
	DirsMap         map[string]bool
	MapSize         int
	Debug           bool
	syncwrites      chan struct{}
	redis_pool      *redis.Pool
	Xref_link_chan  chan []XREF
	Redis_link_chan chan RedisLink
} // end XL struct

type RedisLink struct {
	src string
	dst string
} // end RedisLink struct

func (c *XL) Load_Xref_Linker(maxworkers int, syncwrites int, do_softlinks bool, queue int, cachedir string, redis_pool *redis.Pool, debug_xref_linker bool) bool {
	if maxworkers < 0 {
		log.Printf("Load_Xref_Linker disabled: maxworkers < 0 ")
		return false
	}
	if cachedir == "" {
		log.Printf("ERROR Load_Xref_Linker cachedir not set")
		return false
	}

	if maxworkers == 0 {
		maxworkers = 1
	}
	if syncwrites == 0 {
		syncwrites = maxworkers
	}
	if queue == 0 {
		queue = maxworkers
	}

	c.mux.Lock()
	if c.DirsMap == nil {
		do_redis := false
		if redis_pool != nil {
			XrefLinker.redis_pool = redis_pool
			XrefLinker.Redis_link_chan = make(chan RedisLink, queue)
			do_redis = true
		}
		c.Debug = debug_xref_linker
		log.Printf("Start Load_Xref_Linker maxworkers=%d syncwrites=%d queue=%d cachedir='%s' redis_pool='%v' debug_xref_linker=%t", maxworkers, syncwrites, queue, cachedir, redis_pool, debug_xref_linker)

		c.DirsMap = make(map[string]bool, c.MapSize)
		if c.MapSize == 0 {
			c.MapSize = 512 // set initial map size for direxists cache
		}

		c.syncwrites = make(chan struct{}, syncwrites)
		for i := 1; i <= syncwrites; i++ {
			c.syncwrites <- struct{}{}
		}

		XrefLinker.Xref_link_chan = make(chan []XREF, queue)
		for xid := 1; xid <= maxworkers; xid++ {
			go XrefLinker.xrefs_link_worker(xid, cachedir, do_softlinks, do_redis, true)
			utils.BootSleep()
		}
		if do_redis {
			for rid := 1; rid <= maxworkers; rid++ {
				go XrefLinker.redis_link_worker(rid, nil, true)
				utils.BootSleep()
			}
		}

		log.Printf("OK Load_Xref_Linker Xref_link_chan='%v'", XrefLinker.Xref_link_chan)
	}
	c.mux.Unlock()
	return true
} // end func Load_Xref_Linker

func (c *XL) xrefs_link_worker(xid int, cachedir string, do_softlinks bool, do_redis bool, boot bool) {
	maxlinks, did := 10000, 0
	start := utils.UnixTimeMicroSec()
	if boot {
		log.Printf("Boot xrefs_link_worker xid=%d", xid)
	}
	StorageCounter.Inc("xref_link_worker")
	defer StorageCounter.Dec("xref_link_worker")

	stop := false
xref_linker:
	for {
		if did >= maxlinks {
			break xref_linker
		}
		select {
		case xrefs, ok := <-XrefLinker.Xref_link_chan: /* <<-- []XREF */
			if !ok {
				log.Printf("xrefs_link_worker %d) Xref_link_chan closed: STOP SIGNAL", xid)
				stop = true
				break xref_linker
			}
			if c.Debug {
				log.Printf("xrefs_link_worker %d) xrefs=%d", xid, len(xrefs))
			}

			for _, xref := range xrefs {

				groupdir_head := cachedir + "/head/groups/" + xref.Grouphash
				groupdir_body := cachedir + "/body/groups/" + xref.Grouphash

				if !c.map_dir(groupdir_head) || !c.map_dir(groupdir_body) {
					// re-queue to death
					XrefLinker.Xref_link_chan <- xrefs
					continue xref_linker
				}

				c1, c2, c3 := Get_cache_dir(xref.Msgidhash, "xrefs_link_worker")
				headfile := cachedir + "/head/" + c1 + "/" + c2 + "/" + c3 + "/" + xref.Msgidhash[3:] + ".head"
				bodyfile := cachedir + "/body/" + c1 + "/" + c2 + "/" + c3 + "/" + xref.Msgidhash[3:] + ".body"

				groupfile_head := fmt.Sprintf("%s/%d", groupdir_head, xref.Msgnum)
				groupfile_body := fmt.Sprintf("%s/%d", groupdir_body, xref.Msgnum)

				if do_redis {
					XrefLinker.Redis_link_chan <- RedisLink{groupfile_head, headfile}
					XrefLinker.Redis_link_chan <- RedisLink{groupfile_body, bodyfile}
				}

				if do_softlinks {

					<-c.syncwrites
					if err := os.Symlink(headfile, groupfile_head); err != nil {
						// re-queue to death ... maybe any one can fix permission or problem while app is still running?
						var return_XREF []XREF
						return_XREF = append(return_XREF, xref)
						XrefLinker.Xref_link_chan <- return_XREF
						utils.DebugSleepS(1)
						log.Printf("ERROR xref head softlink src='%s' dst='%s'", headfile, groupfile_head)
					}

					if err := os.Symlink(bodyfile, groupfile_body); err != nil {
						// re-queue to death ... maybe any one can fix permission or problem while app is still running?
						var return_XREF []XREF
						return_XREF = append(return_XREF, xref)
						XrefLinker.Xref_link_chan <- return_XREF
						utils.DebugSleepS(1)
						log.Printf("ERROR xref body softlink src='%s' dst='%s'", bodyfile, groupfile_body)
					}
					c.syncwrites <- struct{}{}
				} // end if do_softlinks
			} // end for range  xrefs
			did++
		} // end select
	} // end forever
	took := utils.UnixTimeMicroSec() - start
	tooks := start - start
	rate_s := tooks
	if took >= 1000*1000 {
		tooks = took / 1000 / 1000
		rate_s = int64(did) / tooks
	}
	log.Printf("Restart xrefs_link_worker %d) did=%d took=(%d µs) tooks=(%d s) rate=%d/s", xid, did, took, tooks, rate_s)
	//time.Sleep(time.Duration(isleep) * time.Millisecond) // interrupt
	if !stop {
		go c.xrefs_link_worker(xid, cachedir, do_softlinks, do_redis, false)
	}
} // end func xrefs_link_worker

func (c *XL) redis_link_worker(rid int, redis_conn redis.Conn, boot bool) {
	maxlinks, did := 10000, 0
	start := utils.UnixTimeMicroSec()

	if boot {
		log.Printf("Boot redis_link_worker rid=%d", rid)
	}
	StorageCounter.Inc("redis_link_worker")
	defer StorageCounter.Dec("redis_link_worker")

	do_redis := false
	if redis_conn != nil {
		do_redis = true

	} else if XrefLinker.redis_pool != nil && redis_conn == nil {
		redis_conn = XrefLinker.redis_pool.Get()
		if redis_conn == nil {
			log.Printf("ERROR redis_link_worker redis_conn=nil")
		} else {
			do_redis = true
		}
	}

	if !do_redis {
		log.Printf("ERROR redis_link_worker XrefLinker.redis_pool='%v' redis_conn='%v'", XrefLinker.redis_pool, redis_conn)
		utils.DebugSleepS(1)
		go XrefLinker.redis_link_worker(rid, redis_conn, true)
		return
	}

	stop := false
for_redis_linker:
	for {
		select {
		case link, ok := <-XrefLinker.Redis_link_chan:
			if !ok {
				log.Printf("Redis_link_chan closed")
				stop = true
				break for_redis_linker
			}
			if _, err := redis_conn.Do("SET", link.src, link.dst); err != nil {
				// re-queue to death
				log.Printf("ERROR redis_link_worker REDIS err='%v'", err)
				XrefLinker.Redis_link_chan <- link
				utils.DebugSleepS(1)
				redis_conn = nil
			}
			did++
			if did >= maxlinks {
				break for_redis_linker
			}
		} // end select
	} // end for_redis_linker

	took := utils.UnixTimeMicroSec() - start
	tooks := start - start
	rate_s := tooks
	if took >= 1000*1000 {
		tooks = took / 1000 / 1000
		rate_s = int64(did) / tooks
	}
	if !stop {
		log.Printf("Restart redis_link_worker %d) did=%d took=(%d µs) tooks=(%d s) rate=%d/s", rid, did, took, tooks, rate_s)
		go XrefLinker.redis_link_worker(rid, redis_conn, false)
	}
} // end func redis_link_worker

func (c *XL) map_dir(dir string) bool {
	// in this map we cache dirs which exist so we have much less checks to filesystem
	retval := false
	c.mux.Lock()
	if len(c.DirsMap) == c.MapSize {
		log.Printf("storage.map_dir need grow len(c.DirsMap)=%d c.MapSize=%d", len(c.DirsMap), c.MapSize)
		//map is full, copy and grow map
		newMapSize := c.MapSize + 512
		newMap := make(map[string]bool, newMapSize)
		for key, val := range c.DirsMap {
			newMap[string(key)] = bool(val)
		}
		c.DirsMap, c.MapSize = newMap, newMapSize
		log.Printf("storage.map_dir grown len(c.DirsMap)=%d c.MapSize=%d", len(c.DirsMap), c.MapSize)
	}

	if !c.DirsMap[dir] { // dir is not cached in map
		if utils.DirExists(dir) { // check if dir exists on filesystem
			c.DirsMap[dir] = true // dir exists, set map dir to true
			retval = true
		} else {
			// dir did not exists
			if utils.Mkdir(dir) { // create dir
				c.DirsMap[dir] = true // dir created, set map dir to true
				retval = true
			} else {
				log.Printf("ERROR storage.map_dir Mkdir dir='%s'", dir)
			}
		}
	} else {
		// dir is cached in map and should exist, return true and worker will process xref linking
		retval = true
	}
	c.mux.Unlock()
	return retval
} // end func map_dir
