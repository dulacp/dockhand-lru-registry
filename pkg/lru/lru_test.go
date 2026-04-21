package lru

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// newTestCache creates a Cache backed by a temp BoltDB file. Caller must call cleanup().
func newTestCache(t *testing.T) (*Cache, func()) {
	t.Helper()
	dir := t.TempDir()
	db, err := bolt.Open(filepath.Join(dir, "usage.db"), 0600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	cache := &Cache{Db: db}
	if err := cache.Init(); err != nil {
		_ = db.Close()
		t.Fatalf("cache.Init: %v", err)
	}
	return cache, func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}
}

// TestAddOrUpdate_SameSecondCollision demonstrates the LRU key-collision bug:
// AccessBucket keys are time.RFC3339-formatted (second-resolution). In
// production, proxy.serveProxy calls AddOrUpdate with time.Now() — which has
// nanosecond precision, but once the Time is Format'd with RFC3339 the
// sub-second precision is lost. Multiple AddOrUpdate calls within one second
// produce identical keys -> only the last Put survives in AccessBucket.
//
// Reproduces what BuildKit's `docker buildx bake --push` causes: a single bake
// pushes many tags within one second, but the LRU only tracks one of them.
func TestAddOrUpdate_SameSecondCollision(t *testing.T) {
	cache, cleanup := newTestCache(t)
	defer cleanup()

	// Mirror production: each AddOrUpdate uses time.Now() with nanosecond
	// precision. With RFC3339 formatting, all five of these collapse to the
	// same bucket key. With RFC3339Nano, they stay distinct.
	names := []string{"repo-a", "repo-b", "repo-c", "repo-d", "repo-e"}
	for _, name := range names {
		img := &Image{Repo: name, Tag: "latest", AccessTime: time.Now()}
		cache.AddOrUpdate(img)
	}

	got := cache.GetLruList()
	if len(got) != len(names) {
		t.Errorf("same-second AddOrUpdate: GetLruList returned %d images, want %d\n"+
			"AccessBucket key format must preserve sub-second precision to avoid collisions.\n"+
			"got list: %+v", len(got), len(names), got)
	}
}

// TestAddOrUpdate_DistinctSecondsAllTracked is the control: when access times
// are in distinct seconds, all images are correctly tracked.
func TestAddOrUpdate_DistinctSecondsAllTracked(t *testing.T) {
	cache, cleanup := newTestCache(t)
	defer cleanup()

	base := time.Date(2026, 4, 21, 18, 49, 47, 0, time.UTC)
	names := []string{"repo-a", "repo-b", "repo-c", "repo-d", "repo-e"}
	for i, name := range names {
		img := &Image{Repo: name, Tag: "latest", AccessTime: base.Add(time.Duration(i) * time.Second)}
		cache.AddOrUpdate(img)
	}

	got := cache.GetLruList()
	if len(got) != len(names) {
		t.Errorf("distinct-second AddOrUpdate: GetLruList returned %d, want %d (this control should pass)", len(got), len(names))
	}
}
