package storage

import (
	"bufio"
	"fmt"
	"github.com/go-while/go-utils"
	"github.com/gomodule/redigo/redis"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	WriteCache               WC
	lock_write_cache_history chan struct{}
)

type CacheItem struct {
	Msgidhash string
	Head      bool
	Lines     *[]string
	Bytes     *[]byte // should already have "\n" (or "\r\n") as end of line!
	Size      int
	Retchan   chan int
} // end CacheItem struct

type WC struct {
	mux                    sync.Mutex
	bufio_max              int
	Debug                  bool
	history_logdir         string
	redis_pool             *redis.Pool
	cachedir               string
	WC_body_chan           chan CacheItem
	WC_head_chan           chan CacheItem
	Log_cache_history_chan chan *[]string
	do_write_cachelog      bool
	fh_cache_history       *os.File
	fh_cache_history_hour  int
	dw_cache_history       *bufio.Writer
}

func (wc *WC) Load_Writecache(head_wc_workers uint64, wc_head_cache_max int, bodycache_workers uint64, wc_body_cache_max int, cachedir string, history_logdir string, redis_pool *redis.Pool, bufio_max int, debug_flag bool) {
	wc.mux.Lock()
	defer wc.mux.Unlock()

	if cachedir == "" {
		log.Printf("ERROR Load_Writecache cachedir not set")
		os.Exit(1)
	}

	if wc.cachedir != "" {
		log.Printf("ERROR Load_Writecache can not boot twice")
		return
	}

	wc.Debug = debug_flag

	if cachedir != "" && wc.cachedir == "" {
		wc.cachedir = cachedir
	}

	if history_logdir != "" && wc.history_logdir == "" {
		if !utils.DirExists(history_logdir) {
			log.Printf("ERROR Load_Writecache !DirExists history_logdir='%s'", history_logdir)
			os.Exit(1)
		}
		wc.do_write_cachelog = true
		wc.history_logdir = history_logdir
		go wc.cache_history_worker()
	}

	if wc.redis_pool == nil && redis_pool != nil {
		wc.redis_pool = redis_pool
	}

	if wc.Log_cache_history_chan == nil {
		wc.Log_cache_history_chan = make(chan *[]string, head_wc_workers+bodycache_workers)
	}

	if bufio_max > 0 {
		wc.bufio_max = bufio_max
	} else {
		wc.bufio_max = 32 * 1024
	}
	// values set, create channels and boot workers
	var wid uint64

	if wc.WC_head_chan == nil && head_wc_workers > 0 && wc_head_cache_max > 0 {
		StorageCounter.Set("head_writecache_worker_max", head_wc_workers)
		log.Printf("Load_Writecache head_wc_workers=%d wc_head_cache_max=%d", head_wc_workers, wc_head_cache_max)
		wc.WC_head_chan = make(chan CacheItem, wc_head_cache_max)
		for wid = 1; wid <= head_wc_workers; wid++ {
			go wc.writecache_worker(wid, "head", wc.WC_head_chan)
			utils.BootSleep()
		}
	}

	if wc.WC_body_chan == nil && bodycache_workers > 0 && wc_body_cache_max > 0 {
		StorageCounter.Set("body_writecache_worker_max", bodycache_workers)
		log.Printf("Load_Writecache bodycache_workers=%d wc_body_cache_max=%d", bodycache_workers, wc_body_cache_max)
		wc.WC_body_chan = make(chan CacheItem, wc_body_cache_max)
		for wid = 1; wid <= bodycache_workers; wid++ {
			go wc.writecache_worker(wid, "body", wc.WC_body_chan)
			utils.BootSleep()
		}
	}

} // end func Load_Writecache

func (wc *WC) writecache_worker(wid uint64, wType string, wc_chan chan CacheItem) {
	maxworker := StorageCounter.Get(wType + "_writecache_worker_max")
	if wid > maxworker {
		return
	}
	StorageCounter.Inc(wType + "_writecache_worker")
	start := utils.UnixTimeSec()
	writes, written, maxwrites := 0, 0, 10000
	stop := false
for_wc:
	for {
		if writes >= maxwrites {
			break for_wc
		}
		select {
		case object, ok := <-wc_chan:
			if !ok {
				log.Printf("STOP WC wType=%s wid=%d", wType, wid)
				stop = true
				break for_wc
			}
			if object.Lines == nil || len(*object.Lines) == 0 {
				log.Printf("ERROR WC wType=%s wid=%d msgid='%s' lines=%d size=%d", wType, wid, object.Msgidhash, len(*object.Lines), object.Size)
				continue for_wc
			}
			if len(object.Msgidhash) < 32 { // allows at least md5
				log.Printf("ERROR WC wType=%s wid=%d len(Msgidhash)=%d < 32", wType, wid, len(object.Msgidhash))
				continue for_wc
			}
			if object.Size == 0 {
				// no size supplied?! ok we calc it...
				if len(*object.Bytes) > 0 {
					object.Size += len(*object.Bytes)
				} else if len(*object.Lines) > 0 {
					for _, line := range *object.Lines {
						object.Size += len(line)
					}
				} else {
					log.Printf("ERROR WC wType=%s wid=%d msgid='%s' lines=%d size=%d", wType, wid, object.Msgidhash, len(*object.Lines), object.Size)
					continue for_wc
				}
			}

			item_wrote_bytes := wc.write_cache(wid, object.Msgidhash, object.Head, object.Lines, object.Size)

			if item_wrote_bytes > 0 {
				writes++
				written += item_wrote_bytes
			} else {
				log.Printf("ERROR wc.write_cache hash='%s' returned item_wrote_bytes=%d", object.Msgidhash, item_wrote_bytes)
			}

			// retchan := make(chan int, 1)
			if object.Retchan != nil {
				// returns an answer to sender
				object.Retchan <- item_wrote_bytes
			}

		} // end select
	} // end for forever
	wc.update_wc_stats(wid, wType, uint64(written), uint64(writes), start)
	StorageCounter.Dec(wType + "_writecache_worker")
	if !stop {
		go wc.writecache_worker(wid, wType, wc_chan)
	}
} // end func writeCache

// write_cache() writes a single head or body to storage
// pass bytes (utils.Lines2Bytes(lines)) or lines ([]string) to this function
// using either ioutil.Writefile or bufio.WriteString
func (wc *WC) write_cache(wid uint64, msgidhash string, is_head bool, lines *[]string, size int) int {
	TIMEDIR_FORMAT := false
	//time_dir, hour_dir := "", ""
	var wrote_bytes int

	start := utils.UnixTimeMicroSec()

	c1, c2, c3 := Get_cache_dir(msgidhash, "wc.write_cache")

	cachedir := ""
	filename, log_filename := msgidhash[3:], ""

	/*
		if TIMEDIR_FORMAT {
			currentTime := time.Now()
			today := fmt.Sprintf("%s", currentTime.Format("2006-01-02"))
			thisH := fmt.Sprintf("%s", currentTime.Format("15"))
			hour_dir = today + "/" + thisH
		}
	*/
	if is_head {
		cachedir = wc.cachedir + "/" + "head" + "/" + c1 + "/" + c2 + "/" + c3 + "/"
		log_filename = msgidhash + ".head"
		if !TIMEDIR_FORMAT {
			filename = cachedir + filename + ".head"
		} /* else {
			time_dir = wc.cachedir + "/" + "head" + "/" + hour_dir + "/"
			_ = os.MkdirAll(time_dir, 0755) // FIXME if !exist dir => create dir => add check map to avoid useless checks/mkdirs
			filename = time_dir + filename + ".head"
		}*/
	} else {
		cachedir = wc.cachedir + "/" + "body" + "/" + c1 + "/" + c2 + "/" + c3 + "/"
		log_filename = msgidhash + ".body"
		if !TIMEDIR_FORMAT {
			filename = cachedir + filename + ".body"
		} /* else {
			time_dir = wc.cachedir + "/" + "body" + "/" + hour_dir + "/"
			_ = os.MkdirAll(time_dir, 0755) // FIXME if !exist dir , create dir, check map to avoid useless checks/mkdirs
			filename = time_dir + filename + ".body"
		}*/
	}

	//filename_tmp := filename + ".tmp"

	if lines != nil {

		if file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644); err == nil {

			bufsize := size + len(*lines)
			if bufsize >= wc.bufio_max {
				bufsize = wc.bufio_max
			}
			datawriter := bufio.NewWriterSize(file, bufsize)
			for _, line := range *lines {
				if DOTSTUFFING {
					// dotwriter: escape leading dot
					if strings.HasPrefix(line, ".") {
						line = "." + line // add escape dot to line
					}
				}
				if n, err := datawriter.WriteString(line + "\n"); err != nil {
					log.Printf("ERROR wc.write_cache datawriter.Write err='%v'", err)
					file.Close()
					return 0
				} else {
					wrote_bytes += n
				}
			}

			if err := datawriter.Flush(); err != nil {
				log.Printf("ERROR wc.write_cache datawriter.Flush err='%v'", err)
				return 0
			}

			if err := file.Close(); err != nil {
				log.Printf("ERROR wc.write_cache file.Close err='%v'", err)
				return 0
			}

			/*if err := os.Rename(filename_tmp, filename); err != nil {
				log.Printf("ERROR wc.write_cache move failed .tmp to file='%s' err='%v'", err, filename)
				return 0
			}*/

			if wc.Debug {
				log.Printf("[ WC ]: wrote file='%s' bytes=%d  took=(%d µs)", log_filename, wrote_bytes, utils.UnixTimeMicroSec()-start)
			}

			// todo: do_write_cachelog
			//if wc.do_write_cachelog {
			//	wc.Log_cache_history_chan <- &[]string{fmt.Sprintf("%s:%d", log_filename, wrote_bytes)}
			//}

		} else {// end open file
			log.Printf("ERROR wc.write_cache CREATE file='%s' err='%v'", filename, err)
		}

	} // end writefile

	return wrote_bytes
} // end func write_cache

func (wc *WC) WC_Worker_UP(wType string) {
	StorageCounter.Inc(wType + "_writecache_worker_max")
} // end func storage.WC_Worker_UP

func (wc *WC) WC_Worker_DN(wType string) {
	StorageCounter.Dec(wType + "_writecache_worker_max")
} // end func storage.WC_Worker_DN

func (wc *WC) WC_Worker_Set(wType string, new_maxworkers uint64) {
	wc.mux.Lock()
	defer wc.mux.Unlock()
	old_maxworkers := StorageCounter.Get(wType + "_writecache_worker_max")
	StorageCounter.Set(wType+"_writecache_worker_max", uint64(new_maxworkers))
	if new_maxworkers > old_maxworkers {
		var wc_chan chan CacheItem
		switch wType {
		case "head":
			wc_chan = wc.WC_head_chan
		case "body":
			wc_chan = wc.WC_body_chan
		}
		for wid := old_maxworkers + 1; wid <= new_maxworkers; wid++ {
			go wc.writecache_worker(wid, wType, wc_chan)
			utils.BootSleep()
		}
	}
} // end func storage.WC_Worker_Set

func (wc *WC) update_wc_stats(wid uint64, wType string, written uint64, writes uint64, start int64) {
	StorageCounter.Add(wType+"_cache_total_wrote_bytes", written)
	StorageCounter.Add(wType+"_cache_total_writes", writes)
	cache_size := 0
	switch wType {
	case "head":
		cache_size = len(wc.WC_head_chan)
	case "body":
		cache_size = len(wc.WC_body_chan)
	}
	log.Printf("STATS WC wType=%s wid=%d bytes=%d writes=%d cache_size=%d", wType, wid, written, writes, cache_size)
} // end func update_wc_stats

func (wc *WC) cache_history_worker() {
	if wc.dw_cache_history == nil {
		log.Printf("ERROR cache_history_worker wc.dw_cache_history=nil")
		return
	}

	lastflush, wrote_lines, max := utils.UnixTimeSec(), 0, 200
forever:
	for {
		select {
		case lines, ok := <-wc.Log_cache_history_chan:
			if !ok {
				log.Print("cache_history_worker STOP SIGNAL")
				break forever
			}
			if lines == nil {
				continue
			}
			wc.write_cache_history(lines)
			wrote_lines += len(*lines)

			if wrote_lines > max || lastflush < utils.UnixTimeSec()-5 {
				if err := wc.dw_cache_history.Flush(); err != nil {
					log.Printf("ERROR wc.write_cache_history Flush err='%v'", err)
				}
				lastflush, wrote_lines = utils.UnixTimeSec(), 0
			}

		} // end select
	} // end forever

	/*
		if err := wc.dw_cache_history.Flush(); err != nil {
			log.Printf("ERROR wc.cache_history_worker Flush err='%v'", err)
		}
		if err := wc.fh_cache_history.Close(); err != nil {
			log.Printf("ERROR wc.cache_history_worker fh.Close err='%v'", err)
		}
	*/
} // end func cache_history_worker

func (wc *WC) write_cache_history(writelog *[]string) {
	if writelog == nil || len(*writelog) == 0 {
		return
	}

	if err := wc.filehandle_cache_history(); err != nil {
		log.Printf("ERROR wc.write_cache_history filehandle_cache_history err='%v'", err)
		return
	}

	for _, logline := range *writelog {
		if _, err := wc.dw_cache_history.WriteString(logline + "\n"); err != nil {
			log.Printf("ERROR wc.write_cache_history: history write failed err='%v'", err)
			break
		}
	} // end for writelog

} // end write_history

func (wc *WC) STOP_WC() {
	// before calling STOP_WC: be sure frontend stopped pushing articles!

	for {
		l1 := len(wc.WC_head_chan)
		l2 := len(wc.WC_body_chan)
		if l1 == 0 && l2 == 0 {
			// force writecache workers to stop
			// if frontend still trys to send:
			//  app will crash with impossible send to closed channels!
			close(wc.WC_head_chan)
			close(wc.WC_body_chan)
			log.Printf("STOP_WC() closed WC_head_chan + WC_body_chan")
			break
		}
		log.Printf("STOP_WC() waiting... WC_head_chan=%d WC_body_chan=%d", l1, l2)
		utils.SleepS(1)
	}

	for {
		w1 := StorageCounter.Get("head_writecache_worker")
		w2 := StorageCounter.Get("body_writecache_worker")
		if w1 == 0 && w2 == 0 {
			log.Printf("STOP_WC() quit head_workers + body_workers", w1, w2)
			break
		}
		log.Printf("STOP_WC() waiting... head_worker=%d body_worker=%d", w1, w2)
		utils.SleepS(1)
	}

	if wc.do_write_cachelog {
		wc.close_cache_history(true)
	}

	log.Printf("STOP_WC() returned")
} // end func storage.WriteCache.STOP_WC

func (wc *WC) close_cache_history(stop bool) error {
	if wc.dw_cache_history != nil {
		log.Printf("close_cache_history: wc.dw_cache_history=nil")
		return nil
	}
	if wc.fh_cache_history == nil {
		log.Printf("close_cache_history: wc.fh_cache_history=nil")
		return nil
	}

	if stop {
		if wc.Log_cache_history_chan != nil {
			for {
				utils.SleepS(1)
				q := len(wc.Log_cache_history_chan)
				if q == 0 {
					close(wc.Log_cache_history_chan)
					log.Printf("STOP_WC() closed wc.Log_cache_history_chan")
					break
				}
				log.Printf("STOP_WC() waiting close_cache_history=%d", q)
			}
		}
	} // end if stop

	if err := wc.dw_cache_history.Flush(); err != nil {
		log.Printf("ERROR wc.close_cache_history dw.Flush err='%v'", err)
		return err
	}
	if err := wc.fh_cache_history.Close(); err != nil {
		log.Printf("ERROR wc.close_cache_history fh.Close err='%v'", err)
		return err
	}

	return nil
} // end func storage.WriteCache.Close_cache_history

func (wc *WC) filehandle_cache_history() error {

	t := time.Now()
	hour := t.Hour()

	if wc.fh_cache_history != nil && hour == wc.fh_cache_history_hour {
		// hour is fine and filehandle exists, return nil as no error
		return nil

	} else if wc.fh_cache_history != nil && hour != wc.fh_cache_history_hour {
		// hour has changed, flush and close logfile to open a new one
		if err := wc.close_cache_history(false); err != nil {
			log.Printf("ERROR wc.filehandle_cache_history close err='%v'", err)
			return err
		}
		wc.dw_cache_history, wc.fh_cache_history = nil, nil // unset bufio.datawriter and filehandle
	}

	y, m, d := t.Year(), t.Month(), t.Day()
	file_path := fmt.Sprintf("%s/WC-%d-%d-%d-%d.log", wc.history_logdir, y, m, d, hour)
	fh, err := os.OpenFile(file_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		wc.fh_cache_history = fh                               // sets filehandle
		wc.fh_cache_history_hour = hour                        // sets hour
		wc.dw_cache_history = bufio.NewWriterSize(fh, 32*1024) // sets buffer to flush every 32KB
	}
	return err
} // end func filehandle_cache_history
