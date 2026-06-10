package lru

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/regclient/regclient/types/ref"
	bolt "go.etcd.io/bbolt"
)

// TestCleanupInvariant_EveryLruEntryIsEvictable reproduces the production
// cleanup-loop stall of 2026-06-09/10 (ENT-3103 follow-up).
//
// Older versions tracked by-digest manifest accesses (BuildKit cache-export
// children, HEAD /v2/<repo>/manifests/sha256:<hex>) as pseudo-tags, persisting
// keys like "repo:sha256:<hex>". The cleanup loop turns each LRU entry into a
// reference via CanonicalName + ref.New before calling TagDelete; a digest
// pseudo-tag yields "host/repo:sha256:<hex>", which ref.New rejects as an
// invalid reference. The loop's error path neither deletes the registry object
// nor removes the cache entry, so the LRU list never shrinks: in production the
// loop spun >26,000 iterations holding the maintenance semaphore, 503-ing all
// CI pushes, freeing zero bytes.
//
// The invariant under test: after Init (process startup), every entry returned
// by GetLruList must produce a reference that ref.New accepts. With 91% of the
// production DB (47,941/52,629 keys) digest-keyed, any unevictable entry in the
// list is a liveness bug, not a data quirk.
func TestCleanupInvariant_EveryLruEntryIsEvictable(t *testing.T) {
	dir := t.TempDir()
	db, err := bolt.Open(filepath.Join(dir, "usage.db"), 0600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Seed the DB exactly as production looked before the fix: real tags mixed
	// with digest pseudo-tags written by an older proxy version.
	err = db.Update(func(tx *bolt.Tx) error {
		images, err := tx.CreateBucketIfNotExists(ImageBucket)
		if err != nil {
			return err
		}
		now := []byte(time.Now().Format(time.RFC3339Nano))
		seed := []string{
			"cache/codex-app-api-arm64:latest",
			"cache/codex-app-api-arm64:6be38e95b24106f65f5163a89dbdef84cda02553",
			"24243490396-7/test-stack-auth:sha256:77bcab407d0d081a16ca934cdac4bf45dab8d627c33cdd3ca21f263d5a08641f",
		}
		for _, key := range seed {
			if err := images.Put([]byte(key), now); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed db: %v", err)
	}

	// Process startup.
	cache := &Cache{Db: db}
	if err := cache.Init(); err != nil {
		t.Fatalf("cache.Init: %v", err)
	}

	assertAllEvictable(t, cache, "after Init on a legacy DB")

	// Runtime: a by-digest manifest access must not (re)introduce an
	// unevictable entry either.
	cache.AddOrUpdate(&Image{
		Repo:       "27200470663-1/test-stack-minio",
		Tag:        "sha256:39f1873051e24bbc150fa8185a3eb478ff1d03592f2ae75a2f9b9f2d955f9eff",
		AccessTime: time.Now(),
	})
	assertAllEvictable(t, cache, "after a by-digest AddOrUpdate")

	// The real tags must still be tracked — evictability must come from
	// excluding digest refs, not from emptying the cache.
	if got := cache.GetLruList(); len(got) < 2 {
		t.Errorf("real tags must survive: got %d entries, want >= 2: %+v", len(got), got)
	}
}

func assertAllEvictable(t *testing.T, cache *Cache, when string) {
	t.Helper()
	for _, image := range cache.GetLruList() {
		canonical := image.CanonicalName("127.0.0.1:5000")
		if _, err := ref.New(canonical); err != nil {
			t.Errorf("%s: GetLruList returned unevictable entry %q\n"+
				"  ref.New(%q): %v\n"+
				"  the cleanup loop can neither TagDelete nor Cache.Remove such an entry, so the LRU list never shrinks (infinite spin, semaphore held, pushes 503)",
				when, image.Name(), canonical, err)
		}
	}
}
