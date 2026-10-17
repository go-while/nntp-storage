package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

const test_msgidhash = "abc456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0"

func test_wc(t *testing.T) (*WC, string) {
	t.Helper()
	dir := t.TempDir()
	headdir := filepath.Join(dir, "head", "a", "b", "c")
	if err := os.MkdirAll(headdir, 0755); err != nil {
		t.Fatal(err)
	}
	wc := &WC{cachedir: dir, bufio_max: 4096}
	return wc, filepath.Join(headdir, test_msgidhash[3:]+".head")
}

func TestWriteCacheAtomic(t *testing.T) {
	wc, file := test_wc(t)
	long := []string{"Subject: first version with a long line", "From: a@b", "X-Long: 0123456789012345678901234567890123456789"}
	if n := wc.write_cache(1, test_msgidhash, true, &long, 100); n == 0 {
		t.Fatalf("write failed")
	}
	short := []string{"Subject: second", "From: c@d"}
	if n := wc.write_cache(1, test_msgidhash, true, &short, 20); n == 0 {
		t.Fatalf("write failed")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	// the old code opened without O_TRUNC: the tail of the longer first version stayed in the file
	if string(data) != "Subject: second\nFrom: c@d\n" {
		t.Fatalf("file content=%q", data)
	}
	tmps, _ := filepath.Glob(file + ".tmp*")
	if len(tmps) != 0 {
		t.Fatalf("tmp files left: %v", tmps)
	}
	if wc.RecentWriteError(60) {
		t.Fatalf("RecentWriteError after successful writes")
	}
}

func TestWriteCacheErrorLeavesNoFile(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	wc, file := test_wc(t)
	dir := filepath.Dir(file)
	if err := os.Chmod(dir, 0555); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0755)
	lines := []string{"Subject: x"}
	if n := wc.write_cache(1, test_msgidhash, true, &lines, 10); n != 0 {
		t.Fatalf("write into read-only dir returned %d", n)
	}
	if _, err := os.Stat(file); err == nil {
		t.Fatalf("file exists after failed write")
	}
	if !wc.RecentWriteError(60) || wc.WriteErrors() != 1 {
		t.Fatalf("write error not recorded: recent=%t count=%d", wc.RecentWriteError(60), wc.WriteErrors())
	}
}

func TestWritecacheWorkerNilLines(t *testing.T) {
	// a CacheItem without lines must not panic the worker and must answer Retchan
	wc, _ := test_wc(t)
	StorageCounter.Set("head_writecache_worker_max", 1) // as Load_Writecache does
	ch := make(chan CacheItem, 1)
	retchan := make(chan int, 1)
	ch <- CacheItem{Msgidhash: test_msgidhash, Head: true, Lines: nil, Retchan: retchan}
	close(ch)
	done := make(chan struct{})
	go func() {
		wc.writecache_worker(1, "head", ch)
		close(done)
	}()
	select {
	case got := <-retchan:
		if got != 0 {
			t.Fatalf("retchan=%d want 0", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no answer on Retchan")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("worker did not stop on closed channel")
	}
}
