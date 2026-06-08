/*
Copyright © 2021 BoxBoat

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package lru

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/boxboat/dockhand-lru-registry/pkg/common"
	bolt "go.etcd.io/bbolt"
)

var (
	// ImageBucket maps image name ("repo:tag") -> last access time. It is the
	// single source of truth for what the cleanup loop may evict.
	ImageBucket = []byte("images")

	// legacyAccessBucket was a secondary time->name index used to iterate tags
	// in LRU order. It was lossy (two tags formatting to the same timestamp key
	// overwrote each other) and silently desynced from ImageBucket, hiding the
	// vast majority of tracked tags from eviction. It is no longer written and
	// is dropped from existing databases on Init.
	legacyAccessBucket = []byte("access")
)

type Cache struct {
	Db *bolt.DB
}

type Image struct {
	Repo       string
	Tag        string
	AccessTime time.Time
}

func (image *Image) Name() string {
	return fmt.Sprintf("%s:%s", image.Repo, image.Tag)
}
func (image *Image) CanonicalName(registryAddress string) string {
	if strings.HasPrefix(registryAddress, "http") {
		registryAddress = strings.Split(registryAddress, "://")[1]
	}
	canonicalName := fmt.Sprintf("%s/%s", registryAddress, image.Name())
	return canonicalName
}

func (cache *Cache) Init() error {
	return cache.Db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(ImageBucket); err != nil {
			return fmt.Errorf("create bucket: %v", err)
		}
		if err := tx.DeleteBucket(legacyAccessBucket); err != nil && !errors.Is(err, bolt.ErrBucketNotFound) {
			return fmt.Errorf("drop legacy access bucket: %v", err)
		}
		return nil
	})
}

func (cache *Cache) AddOrUpdate(image *Image) {
	_ = cache.Db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(ImageBucket).Put(
			[]byte(image.Name()),
			[]byte(image.AccessTime.Format(time.RFC3339Nano)),
		)
	})
}

func (cache *Cache) Remove(image *Image) {
	_ = cache.Db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(ImageBucket).Delete([]byte(image.Name())); err != nil {
			common.LogIfError(err)
			return err
		}
		return nil
	})
}

// GetLruList returns every tracked image ordered least-recently-used first.
// It reads ImageBucket directly and sorts by access time, so the result always
// reflects the full tracked set regardless of timestamp collisions — the bug
// that the removed time-keyed access index suffered from.
func (cache *Cache) GetLruList() []Image {
	var images []Image
	_ = cache.Db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(ImageBucket).ForEach(func(k, v []byte) error {
			accessTime := time.Time{}
			if err := accessTime.UnmarshalText(v); err != nil {
				common.LogIfError(err)
				return nil
			}
			name := string(k)
			sep := strings.LastIndex(name, ":")
			if sep < 0 {
				common.Log.Warnf("skipping malformed image key without tag separator: %s", name)
				return nil
			}
			images = append(images, Image{
				Repo:       name[:sep],
				Tag:        name[sep+1:],
				AccessTime: accessTime,
			})
			return nil
		})
	})
	sort.Slice(images, func(i, j int) bool {
		return images[i].AccessTime.Before(images[j].AccessTime)
	})
	return images
}
