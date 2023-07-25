package storage

import (
	"fmt"
	"github.com/go-while/go-utils"
	"log"
	"os"
)

func Make_cache_dir(cachedir string, bodycache bool, headcache bool) {
	if cachedir == "" {
		log.Printf("ERROR storage.Make_cache_dir: cachedir not set")
		os.Exit(1)
	}
	cacheset := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"}
	ok := 0
	target := 16 * 16 * 16
	if !utils.DirExists(cachedir) && !utils.Mkdir(cachedir) {
		log.Printf("ERROR storage.Make_cache_dir: !Mkdir cachedir='%s'", cachedir)
		os.Exit(1)
	} else {
		for _, c1 := range cacheset {
			for _, c2 := range cacheset {
				for _, c3 := range cacheset {
					checked := 0

					if bodycache {
						subdir_body := fmt.Sprintf("%s/body/%s/%s/%s", cachedir, c1, c2, c3)
						if !utils.DirExists(subdir_body) && !utils.Mkdir(subdir_body) {
							os.Exit(1)
						}
						checked++
					}

					if headcache {
						subdir_head := fmt.Sprintf("%s/head/%s/%s/%s", cachedir, c1, c2, c3)
						if !utils.DirExists(subdir_head) && !utils.Mkdir(subdir_head) {
							os.Exit(1)
						}
						checked++
					}

					ok += 1
					//log.Printf("Make_cache_dir: Level 4 %d/%d DIRS OK checked=%d", ok, target, checked)

				} // end for char3
				//log.Printf("Make_cache_dir: Level 3 %d/%d DIRS OK", ok, target)

			} // end for char2
			log.Printf("storage.Make_cache_dir: checking: %d/%d DIRS OK", ok, target)

		} // end for char1
		//log.Printf("Make_cache_dir: Level 1 ok %d/%d", ok, target)

	} // end if if dirExists

	if ok == 16*16*16 {
		log.Printf("storage.Make_cache_dir: checked %d/%d DIRS OK", ok, target)
	} else {
		failed := target - ok
		log.Printf("storage.Make_cache_dir: ERROR %d/%d dirs ok. failed %d", ok, target, failed)
		os.Exit(1)
	}

} // end func Make_cache_dir

func Get_cache_dir(msgidhash string, src string) (string, string, string) {
	c1, c2, c3 := "0", "0", "0"
	len_hash := len(msgidhash)
	if len_hash >= 3 {
		c1 = string(msgidhash[0])
		c2 = string(msgidhash[1])
		c3 = string(msgidhash[2])
	} else {
		log.Printf("ERROR: storage.Get_cache_dir hash='%s' src='%s'", msgidhash, src)
		return "0", "0", "0"
	}
	//log.Printf("Get_cache_dir c1='%s' c2='%s' c3='%s'", c1, c2, c3)
	return c1, c2, c3
} // end func Get_cache_dir
