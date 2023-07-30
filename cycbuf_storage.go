package storage

import (
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/go-while/go-utils"
	"log"
	//"os"
	"sync"
)

var (
	Cycbuf  CBH
	block_names = []string{ "512B", "1K", "2K", "4K", "32K", "64K", "128K", "1M", "4M", "16M", "64M", "128M", "256M", "512M", "1G", "4G" }
	block_sizes = []int{ 512, 1024, 2048, 4096, 32768, 65536, 131072, 1048576, 4194304, 16777216, 67108864, 134217728, 268435456, 536870912, 1073741824, 4294967296 }
)

func helpmap_blocksize() map[string]int {
	helpmap := make(map[string]int)
	for i, k := range block_names {
		helpmap[k] = block_sizes[i]
	}
	return helpmap
} // end func helpmap_blocksize

type CBH struct {
	mux sync.Mutex
	CycbufsDir	string
	Cycbufs 	map[string]CYCBUF
}

type CYCBUF struct {
	// do? we need a sync point when updating cycbuf head/foot
	//mux sync.Mutex

	Cycbuf_path	string

	// 64 chars uniq random string to identify this cycbuf
	Cycbuf_cookie	string

	// defines what this cycbuf stores: [head|body|comb]
	Cycbuf_type		string

	// full size of this cycbuf (cycbuf_header+cycbuf_body+cycbuf_footer)
	Cycbuf_size		int

	// constant (pow^2!) defines blocksize of this cycbuf: 128K - 1G bytes
	Cycbuf_block	int

	// number of cycles per worker area
	// cycbuf_cycles[0] contains no value
	//		or -1 if cycbuf should not cycle
	//			create new one? or die?
	//	cycbuf_cycles[1:] reserved for worker_ids
	Cycbuf_cycles	[]int



	//	spawns this many writers for this cycbuf
	//	constant (pow^2!) 1 - ...
	//	every write worker should only work in its own area
	//	but setting less or more writers laters would overwrite areas?
	Cycbuf_writers	int

	// spawns this many readers for this cycbuf
	// variable 1 - ...
	//	readers can read everywhere
	Cycbuf_readers	int

	// flushing options
	Cycbuf_feb		int		// flushevery bytes
	Cycbuf_fem		int		// flushevery messages
	Cycbuf_fes		int64	// flushevery seconds

	Cycbuf_lastfl	int64	// timestamp of lastflush
	Cycbuf_open		int64	// timestamp of opening
	Cycbuf_MMAP		mmap.MMap
	//	write_workers last write(s) position(s)
	//	cycbuf_pos[0] contains no value
	//	cycbuf_pos[1:] reserved for worker_ids
	Cycbuf_pos		[]int
} // end CYCBUF struct

func (cbh *CBH) Test_cycbuf() bool {
	return cbh.Create_Cycbuf("test", "comb", 1, "1M", 0, 16, 0, 0, 0)
	//return cbh.Create_Cycbuf("test", "head", 2, "1M", 0, 8, 0, 0, 0)
	//return cbh.Create_Cycbuf("test", "body", 4, "1M", 0, 2, 0, 0, 0)
} // end Test_cycbuf

func (cbh *CBH) Load_Cycbuf(cycbuf_dir string) bool {
	cbh.mux.Lock()
	defer cbh.mux.Unlock()
	if cbh.CycbufsDir != "" {
		return false
	}
	if cbh.Cycbufs != nil {
		return false
	}
	cbh.CycbufsDir = cycbuf_dir
	cbh.Cycbufs = make(map[string]CYCBUF)
	return true
} // end func Load_Cycbuf



func (cbh *CBH) Create_Cycbuf(name string, ctype string, size int, block string, cycles int, writers int, feb int, fem int, fes int64) bool {
	cbh.mux.Lock()
	defer cbh.mux.Unlock()

	if name == "" {
		return false
	}
	if ctype != "head" && ctype != "body" && ctype != "comb" {
		return false
	}
	if utils.CheckNumberPowerOfTwo(writers) != 0 {
		log.Printf("ERROR Create_Cycbuf: writers ! pow^2: use 1,2,4,8,16,32,64,128,...")
		return false
	}

	if block == "" {
		block = "4K"
	}

	helpmap := helpmap_blocksize()
	blocksize := helpmap[block]
	if blocksize <= 0 {
		log.Printf("Invalid blocksize:")
		for k, v := range helpmap {
			fmt.Sprintf("%s : %d bytes", k, v)
		}
		return false
	}
	if utils.CheckNumberPowerOfTwo(blocksize) != 0 {
		log.Printf("ERROR Create_Cycbuf: blocksize=%d ! pow^2", blocksize)
		return false
	}

	if cycles > 0 { cycles = 0 } else { cycles = -1 }
	if writers <= 0 { writers = 1 }
	if feb <= 0 { feb = 128*1024 }
	if fem <= 0 { fem = 1000 }
	if fes <= 0 { fes = 5 }

	if size <= 0 {
		size = 1*1024*1024*1024
	} else {
		size = size*1024*1024*1024
	}


	if blocksize > size {
		log.Printf("ERROR Create_Cycbuf: blocksize=%d > size=%d", blocksize, size)
		return false
	}

	total_blocks := size / blocksize
	area_size := size / writers
	blocks_per_area := area_size / blocksize

	var new_cycbuf CYCBUF
	new_cycbuf.Cycbuf_cookie = utils.Hash256(fmt.Sprintf("%d-%s",utils.Nano(), utils.RandomCharsHex(64)))
	new_cycbuf.Cycbuf_type = ctype
	new_cycbuf.Cycbuf_size = size
	new_cycbuf.Cycbuf_block = blocksize
	new_cycbuf.Cycbuf_cycles[0] = cycles
	new_cycbuf.Cycbuf_writers = writers
	new_cycbuf.Cycbuf_feb = feb
	new_cycbuf.Cycbuf_fem = fem
	new_cycbuf.Cycbuf_fes = fes

	log.Printf("Create Cycbuf:")
	log.Printf(" name='%s' cookie='%s'", name, new_cycbuf.Cycbuf_cookie)
	log.Printf(" ctype=%s size=%d writers=%d", ctype, size, writers)
	log.Printf(" area_size=%d blocks_per_area=%d ", area_size, blocks_per_area)
	log.Printf(" blocksize=%d total_blocks=%d", blocksize, total_blocks)

	return true
} // end func Create_Cycbuf
