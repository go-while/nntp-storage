package storage

import (
	"fmt"
	"github.com/go-while/go-utils"
	"github.com/gomodule/redigo/redis"
	"io/ioutil"
	"log"
	"strings"
	"sync"
)

var (
	ReadCache RC
)

type ReadItem struct {
	Fileobj []byte
	Err     error
} // end ReadItem struct

type ReadReq struct {
	Sessionid string
	File_path string
	Cli_chan  chan ReadItem
	Cmdstring string
	AskRedis  bool
} // end ReadReq struct

type RC struct {
	mux          sync.Mutex
	Debug        bool
	redis_pool   *redis.Pool
	redis_expire int
	RC_head_chan chan ReadReq
	RC_body_chan chan ReadReq
} // end RC struct

func (rc *RC) Load_Readcache(head_rc_workers int, rc_head_ncq int, body_rc_workers int, rc_body_ncq int, redis_pool *redis.Pool, redis_expire int, debug_flag bool) {
	rc.mux.Lock()
	defer rc.mux.Unlock()

	rc.Debug = debug_flag

	if redis_pool != nil {
		rc.redis_pool = redis_pool
		switch redis_expire {
		case 0:
			rc.redis_expire = 300
		case -1:
			rc.redis_expire = -1
		default:
			if redis_expire > 0 {
				rc.redis_expire = redis_expire
			}
		}
	} // end if redis_pool

	if rc.RC_head_chan == nil && head_rc_workers > 0 && rc_head_ncq > 0 {
		log.Printf("Load_Readcache: head_rc_workers=%d rc_head_ncq=%d", head_rc_workers, rc_head_ncq)
		rc.RC_head_chan = make(chan ReadReq, rc_head_ncq)
		for wid := 1; wid <= head_rc_workers; wid++ {
			go rc.readcache_worker(wid, "head", rc.RC_head_chan, nil)
			utils.BootSleep()
		}
	}

	if rc.RC_body_chan == nil && body_rc_workers > 0 && rc_body_ncq > 0 {
		log.Printf("Load_Readcache: body_rc_workers=%d rc_body_ncq=%d", body_rc_workers, rc_body_ncq)
		rc.RC_body_chan = make(chan ReadReq, rc_body_ncq)
		for wid := 1; wid <= body_rc_workers; wid++ {
			go rc.readcache_worker(wid, "body", rc.RC_body_chan, nil)
			utils.BootSleep()
		}
	}

} // end func Load_Readcache

func (rc *RC) readcache_worker(wid int, wType string, rc_chan chan ReadReq, redis_conn redis.Conn) {
	StorageCounter.Inc(wType + "_readcache_worker")
	defer StorageCounter.Dec(wType + "_readcache_worker")
	redis_cache_stat := false // true kills your memory

	start := utils.UnixTimeSec()
	var readb, reads, stats, redis uint64
	var maxreads uint64 = 10000 // reports every x reads

	do_redis := false
	if rc.redis_pool != nil && redis_conn == nil {
		redis_conn = rc.redis_pool.Get()
		if redis_conn == nil {
			log.Printf("ERROR Xref_link_worker redis_conn=nil")
		} else {
			do_redis = true
		}
	}

	stop := false
for_rc:
	for {
		if reads >= maxreads {
			update_rc_stats(wid, wType, readb, reads, stats, redis, start)
			readb, reads, stats, redis, start = 0, 0, 0, 0, utils.UnixTimeSec()
			break for_rc
		}
		select {
		case readreq, ok := <-rc_chan:
			if !ok {
				log.Printf("STOP readCache wType=%s wid=%d", wType, wid)
				update_rc_stats(wid, wType, readb, reads, stats, redis, start)
				stop = true
				break for_rc
			}
			reads++
			// got a read-cache request from client

			if readreq.Cmdstring == "stat" {
				ret := make([]byte, 1)
				result := false

				// checks if we want to ask redis if stat request is cached in redis.
				if readreq.AskRedis && do_redis && redis_cache_stat {

					redis_conn.Send("GET", "STAT:"+readreq.File_path)
					redis_conn.Flush()
					if retval, err := redis_conn.Receive(); err != nil {
						log.Printf("ERROR redis_conn.Receive GET STAT:readreq.File_path err='%v'", err)
					} else {
						retstr := fmt.Sprintf("%s", retval)
						if retstr == "S" || retstr == "0" {
							ret, result = []byte(retstr), true
							redis++
						}
					}

				} // end if askredis

				if !result {
					if utils.FileExists(readreq.File_path) {
						ret[0] = 'S' // ok
					} else {
						ret[0] = '0' // not found
					}

					// kill your memory
					// better have some more memory for redis!
					if do_redis && redis_cache_stat { // cache stat request in redis
						if _, err := redis_conn.Do("SET", "STAT:"+readreq.File_path, string(ret[0]), "NX", "EX", rc.redis_expire); err != nil {
							log.Printf("redis SET STAT:readreq.File_path='%s' failed err='%v'", readreq.File_path, err)
						}
					} // end if do_redis

				} // end if !result
				stats++
				readreq.Cli_chan <- ReadItem{ret, nil}

			} else {

				find_file := readreq.File_path
				var err error
				var fileobj []byte

				// AskRedis should only be set to true if we're requesting grouphash/msgnum
				if do_redis && readreq.AskRedis {
					if retval, err := redis_conn.Do("GET", readreq.File_path); err != nil || retval == nil {
						log.Printf("ERROR readcache_worker: redis_conn.Send GET readreq.File_path='%s' err='%v'", readreq.File_path, err)
					} else {
						retstr := fmt.Sprintf("%s", retval)
						if retstr != "" {
							retstr = strings.TrimSpace(retstr)
							find_file = retstr // overwrites find_file containing grouphash/msgnum with cachedir/head|body/...
							log.Printf("readcache_worker redis GOT readreq.File_path='%s' ==> retstr='%s' find_file='%s'", readreq.File_path, retstr, find_file)
						} else {
							log.Printf("readcache_worker not found @redis readreq.File_path='%s' retstr='%s'", readreq.File_path, retstr)
						}
					}
				}

				if fileobj, err = ioutil.ReadFile(find_file); err != nil {
					// read from diskcache failed
					readreq.Cli_chan <- ReadItem{nil, err}
					continue for_rc
				} // end ioutil.ReadFile

				// successfully read file from diskcache
				if rc.Debug {
					log.Printf("[%s] RC Req wType=%s fp='%s'", readreq.Sessionid, wType, readreq.File_path)
				}

				if do_redis && readreq.AskRedis && find_file == readreq.File_path {
					// find_file is not overwritten: request to grouphash/msgnum was not in redis
					// adds link to redis
					// if you do redis: better have some memory!
					if getlink := utils.GetSoftLinkTarget(readreq.File_path); getlink == "" {
						log.Printf("ERROR utils.GSLT readreq.File_path='%s' link empty", readreq.File_path)
					} else {
						redis_conn.Send("SET", readreq.File_path, getlink)
						redis_conn.Flush()
						log.Printf("redis constant LINKING readreq.File_path='%s' ==> find_file='%s'", readreq.File_path, find_file)
					}
				}

				readreq.Cli_chan <- ReadItem{fileobj, nil} // pass answer to read request back to client

				readb += uint64(len(fileobj))
				fileobj = nil

			} // end if else readreq.Cmdstring
		} // end select
	} // end for for_rc

	if !stop {
		go rc.readcache_worker(wid, wType, rc_chan, redis_conn)
	}
} // end func readcache_worker

func update_rc_stats(wid int, wType string, readb uint64, reads uint64, stats uint64, redis uint64, start int64) {
	if readb > 0 {
		StorageCounter.Add(wType+"_cache_total_readb", readb)
	}
	if reads > 0 {
		StorageCounter.Add(wType+"_cache_total_reads", reads)
	}
	if stats > 0 {
		StorageCounter.Add(wType+"_cache_total_stats", stats)
	}
	if redis > 0 {
		StorageCounter.Add(wType+"_cache_total_redis", redis)
	}

	took := utils.UnixTimeMilliSec() - start
	if readb > 0 || reads > 0 || stats > 0 || redis > 0 {
		speedKB := 0
		if readb >= 1024 && took >= 1000 {
			speedKB = int(int64(readb) / took / 1000 / 1024)
		}
		log.Printf("readCache wType=%s wid=%d KByte/s=%d reads=%d readb=%d stats=%d redis=%d", wType, wid, speedKB, reads, readb, stats, redis)
	}
}
