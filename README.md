# nntp-storage

nntp-storage provides an interface via channels to read/write usenet articles from/to storage


Flat file storage stores articles in flat text files: one for head and one for body.

```
CacheDir structure creates 3 levels of directories [0-9a-f]/[0-9a-f]/[0-9a-f]/
```
Files are stored in format:
```
  "cachedir/[head|body]/[0-9a-f]/[0-9a-f]/[0-9a-f]/"+messageidhash[3:]+".[head|body]"
```
First 3 chars are cut from hash and used for directories.

Remaining string (missing first 3 chars) is used as filename.[head|body]

Hash of Message-ID should be md5, sha1, sha256 or better.

Choose wisely or a change may break things!


# Xref_Linker
is needed to access "article|head|body|stat" via MsgNumber
by creating softlinks from constructed Xref info

it creates soft(sym)links to message-ids in
```
"cachedir/[head|body]/groups/"+grouphash+"/"+"Msgnum"
```

Xref_linker eats a *redis.Pool as optional argument.
If you dont want redis: set redis_pool to nil and AskRedis = false

If you pass a *redis.Pool to Xref_linker it will create links in redis as constant (no expiry) key/value.

When crafting read.request: only set .AskRedis = true when

requesting an "article|head|body|stat" via Msgnum, not via message-ID!


```
If you do xref-linking in redis: better have loads of ram and fast storage for redis!

Redis killed my test import machine in one night...

redis-db > 30GB started swapping and bg-save will never complete with default settings

Redis can be enabled later at any time but will only create old entries on read if wasnt found in redis.
One can choose to disable 'do_softlinks' and run only with redis
but disabling redis later is not possible (yet) because grouphash/msgnum links do not exist without redis
and there is yet no code to export data from redis and create symlink on filesystem.
```


## flat_storage_reader:

provides 2 channels of type 'storage.ReadReq':

-> 'storage.ReadCache.RC_head_chan'

-> 'storage.ReadCache.RC_body_chan'

```
type ReadReq struct {
	Sessionid       string
	File_path       string
	Cli_chan        chan ReadItem
	Cmdstring       string
	AskRedis        bool
} // end ReadReq struct
```

## Crafting read requests
```
readreq := storage.ReadReq{}
readreq.Sessionid = "anything"
// the client constructs file_path from message-id hash
readreq.File_path = "/mnt/cache/0/0/0/ffffff.[head|body]"
// create the channel where client will receive reply to readreq
readreq.Cli_chan = make(chan storage.ReadItem, 1)
// Cmdstring in lowercase
readreq.Cmdstring = "[article|head|body|stat]"
readreq.AskRedis = false

// choose correct channel to put the read request for head or body

// send read request for head:
storage.ReadCache.RC_head_chan <- readreq

// send read request for body:
storage.ReadCache.RC_body_chan <- readreq

reply := <- readreq.Cli_chan

// when requesting an article:
// craft 2 requests with File_path .[head|body]
// wait for replies on Cli_chan, create one for each request
// and construct the article yourself.

// reply.Fileobj is a type []byte.
// if requesting "stat":
//  reply.Fileobj[0] is: 'S' ok OR '0' not found

// do not send the reply.Fileobj as []byte to client!!
//   contains LF and no CRLF pair to define end of line.
// get a sendable []string and send lines with CRLF to client
lines := utils.Bytes2Lines(reply.Fileobj)
```


## flat_storage_writer:

provides 2 channels of type 'storage.CacheItem':

-> 'storage.WriteCache.WC_head_chan'

-> 'storage.WriteCache.WC_body_chan'


Pass a CacheItem object with
	Lines []string or Bytes []byte
into the respective channels
and nntp-storage will write head/body to storage.

```
type CacheItem struct {
	Msgidhash       string
	Head            bool
	Lines           []string	// use either []string
	Bytes           []byte		// or []byte but dont supply both!
	Size            int
} // end CacheItem struct

type ReadItem struct {
	Fileobj         []byte
	Err             error
} // end ReadItem struct
```

## Crafting write requests
```
// storage backend will not count bytes again,
// Size has to be supplied from layer above.

head_part := storage.CacheItem{}
head_part.Msgidhash = sha256(article.messageid)
// if object is a head, set head to true, else leave out or set to false.
head_part.Head = true
// Lines of head should be []string, not a MIMEheader map which is sadly not ordered... :/
head_part.Lines = []string 	// use either []string
head_part.Bytes = []byte	// or []byte but dont supply both!
head_part.Size = head.byteSize

body_part := storage.CacheItem{}
body_part.Msgidhash = sha256(article.messageid) // use generated msgidhash from head if we have it here. else create one!
body_part.Lines = []string	// use either []string
body_part.Bytes = []byte	// or []byte but dont supply both!
body_part.Size = body.byteSize

// pass objects to the correct channels or
//      they will get discarded with an error message only
//          without interrupting the system!

storage.WriteCache.WC_head_chan <- head_part

storage.WriteCache.WC_body_chan <- body_part
```

There is no response to sender.

Fire to channel and forget.

Storage writes it, or Houston, we have a problem!


### flat_storage.go
```
/*
 *  SETUP:
 *      import ( "github.com/go-while/nntp-storage" )
		main() {
			cachedir := "/mnt/cache"
			bodycache, headcache := true, true
			storage.StorageCounter.Make_SC()
			storage.Make_cache_dir(cachedir, bodycache, headcache)
		 } // end main
 *
 *
*/
```


### flat_storage_reader.go
```
/*
 *  SETUP:
 *      //import ( "github.com/go-while/nntp-storage" )
		main() {
			rc_debug_flag_head, rc_debug_flag_body := false, false

			var redis_pool *redis.Pool = nil
			redis_expire := 900 // redis_expire used to cache stat request in redis.
								// set redis_expire to -1 to not cache stat requests at all.
								// if set to 0 defaults to 300s
								// better have some memory for redis!

			rc_head_workers, rc_head_ncq := 100, 100000
			rc_body_workers, rc_body_ncq := 100, 100000

			storage.ReadCache.Load_Readcache(rc_head_workers, rc_head_ncq, rc_body_workers, rc_body_ncq, redis_pool, redis_expire, rc_debug_flag_head)

			if storage.ReadCache.RC_head_chan == nil {
				log.Printf("ERROR RC_head_chan=nil")
				os.Exit(1)
			}
			if storage.ReadCache.RC_body_chan == nil {
				log.Printf("ERROR RC_body_chan=nil")
				os.Exit(1)
			}

		} // end main
 *
 *
*/
```

### flat_storage_writer.go
```
/*
 *  SETUP:
 *      //import ( "github.com/go-while/nntp-storage" )
		main() {
			//cachedir := "/mnt/cache"
			cache_index_logdir := "/var/log/cache_index"
			wc_debug_flag_head, wc_debug_flag_body := false, false

			headcache_workers, headcache_max := 100, 1000000
			bodycache_workers, bodycache_max := 100, 1000000

			storage.Load_Writecache(headcache_workers, headcache_max, bodycache_workers, bodycache_max, cachedir, cache_index_logdir, wc_debug_flag_head)

			if storage.WriteCache.WC_head_chan == nil {
				log.Printf("ERROR WC_head_chan=nil")
				os.Exit(1)
			}
			if storage.WriteCache.WC_body_chan == nil {
				log.Printf("ERROR WC_body_chan=nil")
				os.Exit(1)
			}

		} // end main
 *
 *
*/
```


## xref_linker
```
Xref Linker creates softlinks
from 'cachedir/[head|body]/groups/grouphash/msgnum'
to 'cachedir/[head|body]/[a-f0-9]/[a-f0-9]/[a-f0-9]/messageidhash[:3].[head|body]'
```

### xref_linker.go
```
/*
 *  SETUP:
 *      //import ( "github.com/go-while/nntp-storage" )
		main() {
			//cachedir := "/mnt/cache"
			workers := 1
			// syncwrites limits parallel access to filesystem for xref linking
			//  can be less than workers, more does nothing.
			//  you can have more workers pushing to redis and less parallel syncwrites
			syncwrites := workers
			queuesize := 100
			do_softlinks := true
			debug_flag := false
			var redis_pool *redis.Pool = nil

			storage.XrefLinker.Load_Xref_Linker(workers, syncwrites, do_softlinks, queuesize, cachedir, redis_pool, debug_flag)

			if storage.XrefLinker.Xref_link_chan == nil {
				log.Printf("ERROR Xref_link_chan=nil")
				os.Exit(1)
			}
		}
 *
 *
*/
```


# Contributing

Pull requests are welcome.

For major changes, please open an issue first to discuss what you would like to change.

# License

This project is licensed under the MIT License - see the [LICENSE](https://choosealicense.com/licenses/mit/) file for details.

## Author
[go-while](https://github.com/go-while)
