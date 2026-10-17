# nntp-storage

nntp-storage reads and writes usenet articles as flat files, one for the head and one for the body. Requests go through channels to pools of reader and writer workers.

It is used by [nntp-server](https://github.com/go-while/nntp-server) and nntp-proxy.

## Directory layout

```
<cachedir>/head/<h0>/<h1>/<h2>/<hash[3:]>.head
<cachedir>/body/<h0>/<h1>/<h2>/<hash[3:]>.body
```

- **Hash:** `hash` is the hex hash of the Message-ID; nntp-server uses sha256.
- **Directories:** its first 3 characters are the 3 directory levels (16×16×16 = 4096 directories per type), and the rest is the file name. `Make_cache_dir` creates the directories.
- **Changing the hash:** choose the hash function once. Changing it later makes all stored articles unreachable.

## Setup

```go
import "github.com/go-while/nntp-storage"

storage.StorageCounter.Make_SC()
storage.Make_cache_dir("/news/storage", true, true) // bodycache, headcache

// readers: workers and queue size for heads and bodies, redis pool (or nil), redis expire, debug
storage.ReadCache.Load_Readcache(4096, 1, 4096, 1, nil, 900, false)

// writers: workers and queue size for heads and bodies, cachedir, history logdir, redis pool (or nil), bufio size, debug
storage.WriteCache.Load_Writecache(32, 1000, 32, 1000, "/news/storage", "/news/storage", nil, 128*1024, false)
```

## Writing

Send a `CacheItem` to `storage.WriteCache.WC_head_chan` or `WC_body_chan`:

```go
type CacheItem struct {
	Msgidhash string
	Head      bool
	Lines     *[]string // the lines to write, without line endings
	Bytes     *[]byte   // only used to compute Size when Size is 0
	Size      int       // bytes of all lines without line endings
	Retchan   chan int  // optional: receives the written bytes, 0 on error
}

head := []string{"From: someone@example.com", "Subject: hello"}
storage.WriteCache.WC_head_chan <- storage.CacheItem{Msgidhash: hash, Head: true, Lines: &head, Size: size}
```

How the files are written:
- **Format:** each line is written with LF as line ending.
- **Atomic writes:** a file is written to `<file>.tmp.<worker>` and then renamed to its final name. A crash or a write error never leaves a partial file under the final name; a leftover `.tmp.*` file is harmless and can be deleted while the server is stopped.
- **Async:** writes are asynchronous unless you pass a `Retchan` and wait for it, so a sender that does not wait never sees an error.
- **Write errors:** check `storage.WriteCache.RecentWriteError(60)` and stop accepting articles while it is true (nntp-server answers `436` then). `WriteErrors()` returns the count since start.

## Reading

Send a `ReadReq` to `storage.ReadCache.RC_head_chan` or `RC_body_chan` and wait on your channel:

```go
type ReadReq struct {
	Sessionid string
	File_path string          // <cachedir>/[head|body]/<h0>/<h1>/<h2>/<hash[3:]>.[head|body]
	Cli_chan  chan *ReadItem  // reply channel
	Cmdstring string          // "article", "head", "body" or "stat" (lowercase)
	AskRedis  bool            // only with the xref linker in redis mode, for requests by article number
}

type ReadItem struct {
	Fileobj *[]byte // the file content; for "stat": one byte, 'S' found or '0' not found
	Err     error
}

reply := make(chan *storage.ReadItem, 1)
storage.ReadCache.RC_head_chan <- storage.ReadReq{File_path: path, Cli_chan: reply, Cmdstring: "head"}
item := <-reply
```

- **Line endings:** the content has LF line endings. Split it into lines (for example `utils.Bytes2Lines`) and send them to the client with CRLF.
- **Articles:** for an article, request the head and the body and join them yourself.
- **STAT:** `"stat"` reports a file only as found if it is a regular file with a size above 0.

## Dot-stuffing

`storage.DOTSTUFFING` is off by default: the frontend unstuffs incoming lines and stuffs outgoing ones.

## Shutdown

Stop sending write requests first, then call `storage.WriteCache.STOP_WC()`. It waits until both write queues are empty and all writers have finished.

## Xref linker (optional)

`storage.XrefLinker.Load_Xref_Linker(...)` creates symlinks so articles can be read by group and article number:

```
<cachedir>/[head|body]/groups/<grouphash>/<msgnum>  ->  the head/body file of the article
```

- **Redis:** with a `*redis.Pool` it stores the links in redis instead of, or in addition to, the symlinks (`do_softlinks`). Plan for a lot of RAM: a test import filled redis beyond 30 GB in one night.
- **Can't remove redis later:** there is no code yet to export the links from redis to symlinks, so redis can not be switched off later.
- **Not used by nntp-server:** a link that can not be created is queued again without a limit.

## Cyclic buffers

`cycbuf_storage.go` is an experiment for inn2-like cyclic buffers. It does not store articles yet; do not use it.

## Tests

```
go test .
```

## Contributing

Pull requests are welcome.

For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License - see the [LICENSE](https://choosealicense.com/licenses/mit/) file for details.

## Author

[go-while](https://github.com/go-while)
