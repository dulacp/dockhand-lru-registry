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

// TestAddOrUpdate_SameInstantDistinctNamesAllTracked is the core regression for
// the dead-space bug. The old store kept a secondary time->name index and built
// the LRU list from it; any two tags sharing a timestamp key collided and only
// the last survived, so most tracked tags became invisible to eviction. Keying
// off the image name (ImageBucket) instead means even tags pushed at the exact
// same instant — e.g. `docker buildx bake --push` of many tags — are all
// retained.
func TestAddOrUpdate_SameInstantDistinctNamesAllTracked(t *testing.T) {
	cache, cleanup := newTestCache(t)
	defer cleanup()

	instant := time.Date(2026, 4, 21, 18, 49, 47, 0, time.UTC)
	names := []string{"repo-a", "repo-b", "repo-c", "repo-d", "repo-e"}
	for _, name := range names {
		cache.AddOrUpdate(&Image{Repo: name, Tag: "latest", AccessTime: instant})
	}

	got := cache.GetLruList()
	if len(got) != len(names) {
		t.Errorf("same-instant AddOrUpdate: GetLruList returned %d images, want %d\n"+
			"the eviction list must key off image name, not access time, so colliding "+
			"timestamps cannot hide tags from eviction.\ngot: %+v", len(got), len(names), got)
	}
}

// TestGetLruList_OrdersByAccessTimeOldestFirst asserts the list is sorted least
// recently used first (so the cleanup loop evicts the stalest tags), regardless
// of insertion order, and that "repo:tag" keys are parsed back correctly.
func TestGetLruList_OrdersByAccessTimeOldestFirst(t *testing.T) {
	cache, cleanup := newTestCache(t)
	defer cleanup()

	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	// Insert out of chronological order on purpose.
	cache.AddOrUpdate(&Image{Repo: "team/app", Tag: "newest", AccessTime: base.Add(3 * time.Hour)})
	cache.AddOrUpdate(&Image{Repo: "team/app", Tag: "oldest", AccessTime: base})
	cache.AddOrUpdate(&Image{Repo: "team/app", Tag: "middle", AccessTime: base.Add(1 * time.Hour)})

	got := cache.GetLruList()
	wantTags := []string{"oldest", "middle", "newest"}
	if len(got) != len(wantTags) {
		t.Fatalf("GetLruList returned %d images, want %d: %+v", len(got), len(wantTags), got)
	}
	for i, want := range wantTags {
		if got[i].Tag != want {
			t.Errorf("position %d: got tag %q, want %q (list must be oldest-first)", i, got[i].Tag, want)
		}
		if got[i].Repo != "team/app" {
			t.Errorf("position %d: got repo %q, want %q (repo with '/' must round-trip)", i, got[i].Repo, "team/app")
		}
	}
}

// TestAddOrUpdate_RefreshesAccessTimeWithoutDuplicating ensures re-accessing a
// tag updates its access time in place rather than creating a second entry.
func TestAddOrUpdate_RefreshesAccessTimeWithoutDuplicating(t *testing.T) {
	cache, cleanup := newTestCache(t)
	defer cleanup()

	first := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	later := first.Add(48 * time.Hour)
	cache.AddOrUpdate(&Image{Repo: "repo", Tag: "latest", AccessTime: first})
	cache.AddOrUpdate(&Image{Repo: "repo", Tag: "latest", AccessTime: later})

	got := cache.GetLruList()
	if len(got) != 1 {
		t.Fatalf("re-accessing a tag must not duplicate it: got %d entries %+v", len(got), got)
	}
	if !got[0].AccessTime.Equal(later) {
		t.Errorf("access time not refreshed: got %s, want %s", got[0].AccessTime, later)
	}
}

// TestRemove_DropsOnlyTheNamedImage covers the eviction path: Remove must drop
// the targeted tag and leave the rest of the tracked set intact.
func TestRemove_DropsOnlyTheNamedImage(t *testing.T) {
	cache, cleanup := newTestCache(t)
	defer cleanup()

	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	keep := &Image{Repo: "repo", Tag: "keep", AccessTime: base}
	drop := &Image{Repo: "repo", Tag: "drop", AccessTime: base.Add(time.Hour)}
	cache.AddOrUpdate(keep)
	cache.AddOrUpdate(drop)

	cache.Remove(drop)

	got := cache.GetLruList()
	if len(got) != 1 || got[0].Tag != "keep" {
		t.Fatalf("Remove dropped the wrong entries: got %+v, want only repo:keep", got)
	}
}

// TestInit_DropsLegacyAccessBucket verifies the one-time migration: an existing
// database carrying the removed "access" index has it deleted on Init, while the
// authoritative images survive.
func TestInit_DropsLegacyAccessBucket(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "usage.db")
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Seed a legacy DB: an images entry plus a stale "access" bucket.
	err = db.Update(func(tx *bolt.Tx) error {
		images, err := tx.CreateBucketIfNotExists(ImageBucket)
		if err != nil {
			return err
		}
		if err := images.Put([]byte("repo:latest"), []byte(time.Now().Format(time.RFC3339Nano))); err != nil {
			return err
		}
		access, err := tx.CreateBucketIfNotExists(legacyAccessBucket)
		if err != nil {
			return err
		}
		return access.Put([]byte("2026-04-01T00:00:00Z"), []byte("repo:latest"))
	})
	if err != nil {
		t.Fatalf("seed legacy db: %v", err)
	}

	cache := &Cache{Db: db}
	if err := cache.Init(); err != nil {
		t.Fatalf("cache.Init: %v", err)
	}

	_ = db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(legacyAccessBucket) != nil {
			t.Errorf("legacy access bucket should be dropped on Init")
		}
		if tx.Bucket(ImageBucket) == nil {
			t.Errorf("images bucket must survive Init")
		}
		return nil
	})

	if got := cache.GetLruList(); len(got) != 1 || got[0].Tag != "latest" {
		t.Errorf("images must survive the migration: got %+v", got)
	}
}
