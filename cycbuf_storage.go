package storage

/*
 * concurrent cyclic buffers like inn2
 *
 * brainstorming only! code does not really work yet =)
 *
 */

import (
	"bufio"
	"bytes"
	"fmt"
	"encoding/gob"
	//"github.com/edsrzf/mmap-go"
	//"github.com/johnsiilver/golib/mmap"
	"github.com/go-while/nntp-mmap"
	"github.com/go-while/go-utils"
	"golang.org/x/sys/unix"
	"log"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	debug = false
	DefaultCycBufsBaseDir = "/mnt/cb" // without trailing slash!
	DefaultReaders, DefaultWriters int64 = 1, 1
	MinInitSize int64 = 1024 * 1024
	MinGrowSize int64 = 1024 * 1024
	DefaultSize1G int64 = 1024 * 1024 * 1024
	DefaultFlushEveryBytes int64 = 64 * 1024
	DefaultFlushEverySeconds int64 = 5
	DefaultFlushEveryMessages = 200
	CycBufType_Head = 0xAAA
	CycBufType_Body = 0xBBB
	CycBufType_Comb = 0xCCC
)

var (
	// CycBufsDepth values can be:
	//	0 or 16, 256, 4096, 65536
	//
	// if CycBufsDepth == 0:
	//	uses routing from storage config
	//	routing should allow setting min/max size of articles and groups we want in this cycbuf
	//
	// if CycBufsDepth (16 || 256 || 4096):
	//	creates this many CycBufs for hashs 0__ - f__ without the use of routing
	//
	AvailableCycBufsDepths = []int{0, 16, 256, 4096, 65536}
	cs = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"}
	CBH CycBufHandler
)

type CycBufHandler struct {
	mux        sync.Mutex // the global mutex for CycBufHandler
	rwmux      sync.RWMutex // another global but rw mutex for CycBufHandler
	Depth      int /// 0 uses routing from storage.conf, >0 puts hashs in files 0__ - f__
	BaseDir    string
	CycBufs    map[string]*CYCBUF // key: ident, val: pointer to a CYCBUF
	stop       chan struct{} // receives signal to stop handler
}

type area struct {
	minPos int64 // write from minPos to maxPos and rollover or grow cycbuf by: Growby value
	maxPos int64
	offset int64
	Mmap mmap.Map // handle
	file *os.File
} // end area struct

type CYCBUF struct {
	// internal sync points per cycbuf
	mux sync.Mutex
	rwmux sync.RWMutex

	// the ident of this cycbuf must be unique!
	Ident string
	Hash  string

	// location to this cycbuf as full path or relative
	// identified by Cookie string without file extension
	// Path contains: /mnt/cycbuf/{Cookie}
	// later appending .[cycbuf|cycdat]
	Path string

	// 64 chars uniq random string to identify this cycbuf
	// used as fileident.cycbuf in CycBufDir
	Cookie string

	// defines what this cycbuf stores: [head|body|comb]
	Type int

	// initial size of this cycbuf
	InitCycBufSize int64

	// total number of cycles the cycbuf has done (print with 2 decimals)
	Cycles float64

	// if Rollover == true: cycbuf will grow up to InitCycBufSize
	// and revert back to start when reaching the end overwriting old messages
	// if cycbuf has more than 1 writer: every area can rollower on its own
	Rollover bool

	// if Rollover == false: cycbuf will grow by this amount of bytes
	// if set to 0: cycbuf will not grow and server stops accepting articles
	Growby int64

	// spawn this many dedicated readers for this cycbuf
	// can be changed later without problems
	Readers int64

	// spawn this many dedicated writers for this cycbuf
	// every writer writes in own area: InitCycBufSize / Writers
	// note: it is impossible to change writers later!
	Writers int64

	// flushing options
	LastFlush int64 // timestamp
	FlushEveryBytes int64
	FlushEveryMessages int
	FlushEverySeconds int64

	// counter when to flush
	CtrB int
	CtrM int
	CtrS int

	TimeOpen   int64 // timestamp of opening

	//	cb.Offsets[0] contains no value
	//	cb.Offsets[1:] used by writers
	Offsets map[int]*area // key: writerID, val: offset / position in cycbuf-area
} // end CYCBUF struct


func (handler *CycBufHandler) InitCycBufs(basedir string, depth int, initsize int64, growby int64, readers int64, writers int64, mode int, rwtest bool) (bool, error) {
	handler.mux.Lock()
	defer handler.mux.Unlock()

	// ./nntp-server -cycsetup=true -cycdepth=16 -cycsize=128 -cycgrow=0 -cycwriters=1
	// ./nntp-server -cycsetup=true -cycdepth=16 -cycsize=128 -cycgrow=1 -cycwriters=1
	// ./nntp-server -cycsetup=true -cycdepth=256 -cycsize=128 -cycgrow=128 -cycwriters=1
	// ./nntp-server -cycsetup=true -cycdepth=4096 -cycsize=1024 -cycgrow=1024 -cycwriters=1

	if basedir == "" {
		basedir = DefaultCycBufsBaseDir
	}
	if basedir[len(basedir)-1] == '/' {
		return false, fmt.Errorf("ERROR InitCycBufs basedir remove trailing slash")
	}
	if !utils.DirExists(basedir) {
		return false, fmt.Errorf("ERROR InitCycBufs basedir='%s' does not exist", basedir)
	}

	if initsize <= MinInitSize {
		initsize = MinInitSize
	}

	switch mode {
		case 1:
			// separate cycbufs for head and body
		case 2:
			// combined cycbuf
		default:
			return false, fmt.Errorf("ERROR InitCycBufs invalid mode")
	}

	rollover := true // default
	if growby > 0 {
		if growby <= MinGrowSize {
			growby = MinGrowSize
		}
		rollover = false
	}

	if readers <= 0 {
		readers = 1
	}

	if writers <= 0 {
		writers = 1
	}

	if !utils.CheckNumber64PowerOfTwo(initsize) {
		return false, fmt.Errorf("ERROR InitCycBufs initsize must be pow of 2 in MBytes")
	}

	if growby > 0 && !utils.CheckNumber64PowerOfTwo(growby) {
		return false, fmt.Errorf("ERROR InitCycBufs growby must be pow of 2 in MBytes")
	}

	if !utils.CheckNumber64PowerOfTwo(writers) {
		return false, fmt.Errorf("ERROR InitCycBufs writers must be pow of 2")
	}


	vd, idx := false, 0
	for i, d := range AvailableCycBufsDepths {
		if d == depth {
			vd, idx = true, i
			break
		}
	}
	if !vd {
		return false, fmt.Errorf("ERROR InitializeCycBufs: invalid depth")
	}

	if depth == 0 {
		// TODO pre-create cycbufs from storage.conf
		return false, fmt.Errorf("routing to cycbuf not implemented")
	}

	// create 16, 256, 4096 or 65536 cycbufs
	// derived from depth value
	log.Printf("idx=%d", idx)

	var cycbufs []string

	switch idx {
		case 1:
			for _, c1 := range cs {
				cycbufs = append(cycbufs, c1)
			}
		case 2:
			for _, c1 := range cs {
				for _, c2 := range cs {
					cycbufs = append(cycbufs, c1+c2)
				}
			}
		case 3:
			for _, c1 := range cs {
				for _, c2 := range cs {
					for _, c3 := range cs {
						cycbufs = append(cycbufs, c1+c2+c3)
					}
				}
			}
		case 4:
			for _, c1 := range cs {
				for _, c2 := range cs {
					for _, c3 := range cs {
						for _, c4 := range cs {
							cycbufs = append(cycbufs, c1+c2+c3+c4)
						}
					}
				}
			}
		default:
			return false, fmt.Errorf("unsupported depth")
	}

	if len(cycbufs) != AvailableCycBufsDepths[idx] {
		return false, fmt.Errorf("invalid idx")
	}

	log.Printf("Initializing CycBufs=%d idx=%d initsize=[%d MB / cycbuf]=[%d MB total] rollover=%t",
						len(cycbufs)*2, idx, initsize/1024/1024, int64(len(cycbufs))*2*initsize/1024/1024, rollover)

	log.Printf("-> writers=[%d / cycbuf] * growby=[%d MB / writer] ==> growsize=[%d MB / cycbuf]",
					writers, growby/1024/1024, writers*growby/1024/1024)

	log.Printf("-> Concurrent Writers: %d", writers*int64(len(cycbufs))*2)

	log.Printf("... waiting 5 seconds ... to cancel: ctrl+c !")
	time.Sleep(time.Second*5)

	feb, fem, fes := DefaultFlushEveryBytes, DefaultFlushEveryMessages, DefaultFlushEverySeconds

	for _, ident := range cycbufs {
		switch mode {
			case 1:
				// separate cycbufs for head and body
				cycFilePathHeadBuf := basedir+"/"+ident+".hbuf"
				cycFilePathHeadDat := cycFilePathHeadBuf + ".dat"
				cycFilePathBodyBuf := basedir+"/"+ident+".bbuf"
				cycFilePathBodyDat := cycFilePathBodyBuf + ".dat"
				if utils.FileExists(cycFilePathHeadBuf) || utils.FileExists(cycFilePathHeadDat) {
					return false, fmt.Errorf("ERROR InitCycBufs exists='%s'", ident)
				}
				if utils.FileExists(cycFilePathBodyBuf) || utils.FileExists(cycFilePathBodyDat) {
					return false, fmt.Errorf("ERROR InitCycBufs exists='%s'", ident)
				}

				//log.Printf("Init CycBuf:head ident='%s' fp='%s' dp='%s'", ident, cycFilePathHeadBuf, cycFilePathHeadDat)
				cbHead, err1 := handler.CreateCycBuf(ident, CycBufType_Head, cycFilePathHeadBuf, initsize, rollover, growby, feb, fem, fes, readers, writers, rwtest)
				if err1 != nil {
					log.Printf("ERROR InitCycBufs => CreateCycBuf:head ident='%s' failed...\n --> err1='%v' cbHead='%#v'", ident, err1, cbHead)
					os.Exit(1)
				}

				//log.Printf("Init CycBuf:body ident='%s' fp='%s' dp='%s'", ident, cycFilePathBodyBuf, cycFilePathBodyDat)
				cbBody, err2 := handler.CreateCycBuf(ident, CycBufType_Body, cycFilePathBodyBuf, initsize, rollover, growby, feb, fem, fes, readers, writers, rwtest)
				if err2 != nil {
					log.Printf("ERROR InitCycBufs => CreateCycBuf:body ident='%s' failed...\n --> err2='%v' cbBody='%#v'", ident, err2, cbBody)
					os.Exit(1)
				}

				//log.Printf("-> Created CycBuf ident='%s'", ident)
				//log.Printf("   == head='%#v'", cbHead)
				//log.Printf("   == body='%#v'", cbBody)

				// TODO
				/*
				cbHead..wid..Mmap.Close()
				cbHead..wid..file.Close()

				cbBody..wid..Mmap.Close()
				cbBody..wid..file.Close()
				*/

			case 2:
				// combined cycbuf
				cycFilePathBuf := basedir+"/"+ident+".cycbuf"
				cycFilePathDat := cycFilePathBuf + ".dat"
				if utils.FileExists(cycFilePathBuf) || utils.FileExists(cycFilePathDat) {
					return false, fmt.Errorf("ERROR InitCycBufs exists='%s'", ident)
				}
				//log.Printf("Init CycBuf:comb ident='%s' fp='%s' dp='%s'", ident, cycFilePathBuf, cycFilePathDat)
				cbComb, err := handler.CreateCycBuf(ident, CycBufType_Comb, cycFilePathBuf, initsize, rollover, growby, feb, fem, fes, readers, writers, rwtest)
				if err != nil {
					log.Printf("ERROR InitCycBufs => CreateCycBuf:comb ident='%s' failed...\n --> err='%v' cbComb='%#v'", ident, err, cbComb)
					os.Exit(1)
				}
				//log.Printf("-> Created CycBuf ident='%s'", ident)
				//log.Printf("   == comb='%#v'", cbComb)

				// TODO
				/*
				cbComb..wid..Mmap.Close()
				cbComb..wid..file.Close()
				*/

			default:
				return false, fmt.Errorf("ERROR InitCycBufs invalid mode")
		}
	}
	return true, nil
} // end func InitCycBufs

func (handler *CycBufHandler) Load_CycBufs(indexfile string) bool {
	handler.mux.Lock()
	defer handler.mux.Unlock()
	/*
	if handler.CycbufsDir != "" {
		return false
	}
	if handler.CycBufs != nil {
		return false
	}
	handler.Cycbufs = make(map[string]*CYCBUF)
	handler.CycbufsDir = cycbuf_dir
	*/
	return true
} // end func Load_CycBufs

func (handler *CycBufHandler) CreateCycBuf(ident string, ctype int, bufpath string, initsize int64, rollover bool, growby int64, feb int64, fem int, fes int64, readers int64, writers int64, rwtest bool) (bool, error) {

	if ident == "" {
		return false, fmt.Errorf("ERROR CreateCycBuf: ident is empty")
	}

	switch ctype {
		case CycBufType_Head:
			// pass
		case CycBufType_Body:
			// pass
		case CycBufType_Comb: // combined cycbuf (head+body)
			// pass
		default:
			return false, fmt.Errorf("ERROR CreateCycBuf: invalid ctype")
	}

	if initsize <= 0 {
		initsize = DefaultSize1G
	} else {
		initsize = initsize //* DefaultSize1G
	}

	if growby <= 0 {
		growby = 0
	}

	if feb <= 0 {
		feb = DefaultFlushEveryBytes
	}
	if fem <= 0 {
		fem = DefaultFlushEveryMessages
	}
	if fes <= 0 {
		fes = DefaultFlushEverySeconds
	}

	if readers <= 0 {
		readers = 1 // readers
	}
	if writers <= 0 {
		writers = 1 // writers
	}
	if writers > 1 && writers%2 != 0 {
		return false, fmt.Errorf("ERROR writers must be a multiple of 2")
	}

	newCB := &CYCBUF{}
	newCB.Ident = ident
	newCB.Cookie = utils.RandomCharsHex(8)
	newCB.Type = ctype
	newCB.InitCycBufSize = initsize
	newCB.Rollover = rollover
	newCB.Growby = growby
	//newCB.Cycles = 0.00
	newCB.FlushEveryBytes = feb
	newCB.FlushEveryMessages = fem
	newCB.FlushEverySeconds = fes
	newCB.Readers = readers
	newCB.Writers = writers
	newCB.TimeOpen = utils.UnixTimeSec()
	newCB.Offsets = make(map[int]*area)

	log.Printf("CreateCycBuf: ident='%s' cookie='%s' ctype=%d rollover=%t", ident, newCB.Cookie, ctype, rollover)

	areasize := newCB.InitCycBufSize
	if writers == 1 {
		newCB.Offsets[1] = &area{ minPos: 0, maxPos: areasize }
	} else {
		// calculcate initial areas for writers
		areasize = newCB.InitCycBufSize / writers
		if !utils.CheckNumberPowerOfTwo(int(areasize)) {
			return false, fmt.Errorf("ERROR CreateCycBuf calculating areasize failed")
		}
		//log.Printf(" `-> initsize=%d writers=%d areasize=%d", initsize, writers, areasize)
		var minPos int64
		maxPos := areasize
		for wid := 1; wid <= int(writers); wid++ {
			newCB.Offsets[wid] = &area{ minPos: minPos, maxPos: maxPos }
			//log.Printf("  `-> AREA writerid=%d 'minPos >= %d' && 'maxPos < %d'", wid, minPos, maxPos)
			minPos += areasize
			maxPos += areasize
		}
	}
	// pre-allocate the CycBuf
	if err := Fallocate(bufpath, 0, newCB.InitCycBufSize); err != nil {
		log.Printf("ERROR CreateCycBuf Fallocate err='%v'", err)
		return false, err
	}

	if !utils.FileExists(bufpath) {
		return false, fmt.Errorf("ERROR CreateCycBuf Fallocate !FileExists='%s'", bufpath)
	}
	newCB.Path = bufpath
	bufdatPath := newCB.Path + ".dat"
	//log.Printf("writeBufDat newCB=\n '%#v'", newCB)
	// encode and write the bufdat file here
	if errwb := handler.writeBufDat(newCB); errwb != nil {
		log.Printf("ERROR handler.writeBufDat fp='%s' err='%v'", bufdatPath, errwb)
		return false, errwb
	}
	log.Printf("OK handler.writeBufDat fp='%s'", bufdatPath)

	testCB := &CYCBUF{}
	errgts := loadBufDat(bufdatPath, testCB)
	if errgts != nil {
		return false, errgts
	}
	//log.Printf("returned from loadBufDat testCB='%#v'", testCB)

	if !rwtest {
		return true, nil
	}

	// creates 4K long teststring to test writes to cycbuf
	teststring := ""
	for i := 0; i < 1024; i++ {
		teststring = teststring+"0\n0\n"
	}
	//log.Printf("len teststring=%d bytes=%d", len(teststring), len([]byte(teststring)))
	// mmap regions for writers and check for null bytes
	for wid := 1; wid <= int(writers); wid++ {
		fh, err := os.OpenFile(bufpath, os.O_RDWR, 0644)
		defer fh.Close() // dont close and keep open
		if err != nil {
			return false, err
		}
		flag1, flag2 := mmap.RDWR, unix.MAP_SHARED
		mapped, err := mmap.NewMmap(fh, int(newCB.Offsets[wid].maxPos), flag1, flag2, newCB.Offsets[wid].minPos)

		if err != nil {
			log.Printf("ERROR CreateCycBuf MapRegion err='%v'", err)
			return false, err
		}

		//log.Printf(" write wid=%d mapped.Pos=%d len=%d min=%d max=%d", wid, mapped.Pos(), mapped.Len(), newCB.Offsets[wid].minPos, newCB.Offsets[wid].maxPos)

		writes, bytectr := 0, 0
		for i := int64(0); i < areasize/4096; i++ {
			n, err := mapped.Write([]byte(teststring))
			if err != nil /*&& err != io.EOF*/ {
				return false, fmt.Errorf("ERROR CreateCycBuf mapped.Write writes=%d i=%d n=%d bytes=%d err='%v'", writes, i, n, bytectr, err)
			}
			writes++
			bytectr += n
		}
		mapped.FlushMmap()

		//log.Printf("  wid=%d writes=%d bytes=%d mapped.Pos=%d", wid, writes, bytectr, mapped.Pos())
		mapped.Seek(0, 0)
		//log.Printf(" reset wid=%d mapped.Pos=%d areasize=%d", wid, mapped.Pos(), areasize)

		//log.Printf(" reset wid=%d mapped.Pos=%d", wid, mapped.Pos())

		var null int64
		buf := make([]byte, 64*1024)
		fileScanner := bufio.NewScanner(mapped)
		fileScanner.Buffer(buf, 0)
		fileScanner.Split(bufio.ScanLines)
		for fileScanner.Scan() {
			if fileScanner.Text() == "0" {
				// "\n" got removed from line scanner so we count only half of area size
				//log.Printf(" wid=%d null=%d", wid, null)
				null++
			}
		}
		if null != areasize/2 {
			return false, fmt.Errorf("ERROR CreateCycBuf mmap failed\n ---> null=%d pos=%d areasize=%d miss=%d", null, mapped.Pos(), areasize, areasize/2-null)
		}
		mapped.Seek(0, 0)
		nulls := ""
		for i := 0; i < 1024; i++ {
			nulls = nulls+"\x00\x00\x00\x00"
		}
		//log.Printf(" OK counted null=%d is half of areasize, write nulls=%d bytes=%d", null, len(nulls), len([]byte(nulls)))
		for i := int64(0); i < areasize/4096; i++ {
			if n, err := mapped.Write([]byte(nulls)); err != nil {
				log.Printf("ERROR mapped.Write err='%v' i=%d areasize=%d n=%d", err, i, areasize, n)
				os.Exit(1)
			}
		}
		mapped.FlushMmap()
		mapped.Seek(0, 0)
		//newCB.Offsets[wid].Mmap = mapped
		//newCB.Offsets[wid].file = fh
		//log.Printf(" `> OK writer=%d/%d mapped=%d nulls=%d pos=%d", wid, writers, len(mapped.Bytes()), null, mapped.Pos())
		mapped.Close()
	} // end for range writers
	return true, nil
} // end func CreateCycBuf

func Fallocate(filePath string, offset int64, length int64) error {
	if length == 0 {
		return fmt.Errorf("ERROR Fallocate length=0")
	}
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	defer file.Close()
	if err != nil {
		return err
	}

	var mode uint32 = 0
	//var mode uint32 = 2 // unix.FALLOC_FL_KEEP_SIZE
	//var mode uint32 = 3 // unix.FALLOC_FL_KEEP_SIZE | unix.FALLOC_FL_PUNCH_HOLE
	//var mode uint32 = 4 // unix.FALLOC_FL_PUNCH_HOLE
	errf := syscall.Fallocate(int(file.Fd()), mode, offset, length)
	if errf != nil {
		return errf
	}

	fi, err := file.Stat()
	if err != nil {
	  // Could not obtain stat, handle error
	  return err
	}
	log.Printf("Fallocate OK fp='%s' size=(%d MB)", filePath, fi.Size()/1024/1024)
	return nil
} // end func Fallocate: unix version ripped by github.com/detailyang/go-fallocate


func MapRegion(filePath string, minPos int64, maxPos int64) (mmap.Map, *os.File, error) {
	fh, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, err
	}
	flags := unix.MAP_SHARED
	prot := mmap.RDWR
	length := int(maxPos-minPos)
	offset := minPos
	mapped, errm := mmap.NewMmap(fh, length, prot, flags, offset)
	if errm != nil {
		return nil, nil, errm
	}
	return mapped, fh, nil
} // end func MapRegion


func (handler *CycBufHandler) SetOffset(cb *CYCBUF, wid int, offset int64) {
	// will be called by concurrent writers go routines per cycbuf
	cb.mux.Lock()
	if cb.Offsets[wid] == nil {
		cb.Offsets[wid] = &area{ offset: offset }
		//wrote = offset
	} else {
		//wrote = offset - cb.Offsets[wid].offset
		cb.Offsets[wid].offset = offset
	}
	cb.mux.Unlock()
} // end func SetOffset

func (handler *CycBufHandler) writeBufDat(cb *CYCBUF) error {
	cb.mux.Lock()
	defer cb.mux.Unlock()
	if cb.Path == "" {
		return fmt.Errorf("ERROR writeBufDat cb.Path empty")
	}
	fp := cb.Path + ".dat"
	var buf bytes.Buffer
	err := CYCBUF_ToBytes(cb, &buf)
	if err != nil {
		return err
	}
	if len(buf.Bytes()) == 0 {
		return fmt.Errorf("ERROR writeBufDat WriteFile fp='%s' bytes=0")
	}
	log.Printf("writeBufDat fp='%s' bytes=%d", fp, len(buf.Bytes()))
	err = ioutil.WriteFile(fp, buf.Bytes(), 0644)
	if err != nil {
		log.Printf("ERROR writeBufDat WriteFile fp='%s' err='%v'", err)
		return err
	}
	return nil // no error
} // end func writeBufDat

func CYCBUF_ToBytes(input *CYCBUF, output *bytes.Buffer) (error) {
	encoder := gob.NewEncoder(output)
	if err := encoder.Encode(input); err != nil {
		return err
	}
	//log.Printf("CYCBUF_ToBytes returning output=%d", len(output.Bytes()))
	return nil
} // end func CYCBUF_ToBytes

func AnyBytesToGob(input *[]byte, output *bytes.Buffer) (error) {
	encoder := gob.NewEncoder(output)
	if err := encoder.Encode(*input); err != nil {
		return err
	}
	//log.Printf("BytesToGob returning output=%d", output.Bytes())
	return nil
} // end func AnyBytesToGob

func loadBufDat(fp string, output *CYCBUF) (error) {
	// ++ https://gist.github.com/amlwwalker/92d60ae8b27099c166dd013161052c4f
	fh, err := os.Open(fp)
	if err != nil {
		log.Printf("ERROR loadBufDat err='%#v'", err)
		return err
	}
	defer fh.Close()
	decoder := gob.NewDecoder(fh)
	if err := decoder.Decode(output); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Printf("ERROR loadBufDat fp='%s' err='%#v'", fp, err)
			return err
		}
	}
	//log.Printf("loadBufDat output=CYCBUF='%#v'", output)
	return nil // no error
} // end func loadBufDat
