/*
 *
 * Copyright 2020-present Arpabet, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package bboltstorage

import (
	"bytes"
	bolt "go.etcd.io/bbolt"
	"go.arpabet.com/storage"
	"io"
	"os"
)

type boltStorage struct {
	db     *bolt.DB
}

func New(conf *BoltConfig) (storage.ManagedStorage, error) {

	db, err := OpenDatabase(conf)
	if err != nil {
		return nil, err
	}

	return &boltStorage {db}, nil
}

func FromDB(db *bolt.DB) storage.ManagedStorage {
	return &boltStorage {db}
}

func (t* boltStorage) Destroy() error {
	return t.db.Close()
}

func (t* boltStorage) Get() *storage.GetOperation {
	return &storage.GetOperation{Storage: t}
}

func (t* boltStorage) Set() *storage.SetOperation {
	return &storage.SetOperation{Storage: t}
}

func (t* boltStorage) CompareAndSet() *storage.CompareAndSetOperation {
	return &storage.CompareAndSetOperation{Storage: t}
}

func (t* boltStorage) Remove() *storage.RemoveOperation {
	return &storage.RemoveOperation{Storage: t}
}

func (t* boltStorage) Enumerate() *storage.EnumerateOperation {
	return &storage.EnumerateOperation{Storage: t}
}

func (t* boltStorage) GetRaw(bucket, key []byte, ttlPtr *int, versionPtr *int64, required bool) ([]byte, error) {
	return t.getImpl(bucket, key, required)
}

func (t* boltStorage) SetRaw(bucket, key, value []byte, ttlSeconds int) error {

	if t.db.IsReadOnly() {
		return ErrDatabaseReadOnly
	}

	return t.db.Update(func(tx *bolt.Tx) error {

		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}

		return b.Put(key, value)

	})

}

func (t* boltStorage) CompareAndSetRaw(bucket, key, value []byte, ttlSeconds int, version int64) (bool, error) {
	return true, t.SetRaw(bucket, key, value, ttlSeconds)
}

func (t* boltStorage) RemoveRaw(bucket, key []byte) error {

	if t.db.IsReadOnly() {
		return ErrDatabaseReadOnly
	}

	return t.db.Update(func(tx *bolt.Tx) error {

		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}

		return b.Delete(key)
	})

}

func (t* boltStorage) getImpl(bucket, key []byte, required bool) ([]byte, error) {

	var val []byte

	err := t.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}

		val = b.Get(key)
		return nil
	})

	if err != nil {
		return nil, err
	}

	if val == nil && required {
		return nil, os.ErrNotExist
	}

	return val, nil
}

func (t* boltStorage) EnumerateRaw(bucket, seek []byte, batchSize int, onlyKeys bool, cb func(entry *storage.RawEntry) bool) error {

	// for API compatibility with other storage impls (PnP)
	if !bytes.HasPrefix(seek, bucket) {
		return ErrInvalidSeek
	}

	seek = seek[len(bucket):]

	return t.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}

		cur := b.Cursor()
		k, v := cur.Seek(seek)  // within bucket
		if k != nil {
			re := storage.RawEntry{
				Key:     k,
				Value:   v,
				Ttl:     0,
				Version: 0,
			}
			if !cb(&re) {
				return nil
			}
		}

		for k, v := cur.Seek(seek); k != nil; k, v = cur.Next() {
			re := storage.RawEntry{
				Key:     k,
				Value:   v,
				Ttl:     0,
				Version: 0,
			}
			if !cb(&re) {
				return nil
			}
		}

		return nil

	})

}

func (t* boltStorage) FetchKeysRaw(bucket []byte, batchSize int) ([][]byte, error) {

	var keys [][]byte

	t.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		
		return b.ForEach(func(k, v []byte) error {
			keys = append(keys, k)
			return nil
		})

	})

	return keys, nil
}

func (t* boltStorage) Compact(discardRatio float64) error {
	// bolt does not support compaction
	return nil
}

func (t* boltStorage) Backup(w io.Writer, since uint64) (uint64, error) {

	var txId int

	err := t.db.View(func(tx *bolt.Tx) error {
		txId = tx.ID()
		_, err := tx.WriteTo(w)
		return err
	})

	return uint64(txId), err

}

func (t* boltStorage) Restore(src io.Reader) error {

	dbPath := t.db.Path()
	if t.db.IsReadOnly() {
		return ErrDatabaseReadOnly
	}

	err := t.db.Close()
	if err != nil {
		return err
	}

	dst, err := os.OpenFile(dbPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, DataFilePerm)
	if err != nil {
		return err
	}

	_, err = io.Copy(dst, src)
	if err != nil {
		return err
	}
	
	t.db, err = bolt.Open(dbPath, DataFilePerm, &bolt.Options{
		Timeout:         OpenTimeout,
		NoGrowSync:      NoGrowSync,
		ReadOnly:        false,
		MmapFlags:       MmapFlags,
		InitialMmapSize: InitialMmapSize,
	})

	return err
}

func (t* boltStorage) DropAll() error {

	dbPath := t.db.Path()
	if t.db.IsReadOnly() {
		return ErrDatabaseReadOnly
	}

	err := t.db.Close()
	if err != nil {
		return err
	}

	err = os.Remove(dbPath)
	if err != nil {
		return err
	}

	t.db, err = bolt.Open(dbPath, DataFilePerm, &bolt.Options{
		Timeout: OpenTimeout,
	})

	return err
}

func (t* boltStorage) DropWithPrefix(bucket []byte) error {

	return t.db.Update(func(tx *bolt.Tx) error {

		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		
		return b.ForEach(func(k, v []byte) error {
			return b.Delete(k)
		})

	})

}

func (t* boltStorage) Instance() interface{} {
	return t.db
}