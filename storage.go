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

package storage

import (
	"encoding/binary"
	"go.arpabet.com/beans"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"os"
	"reflect"
)

const NoTTL = 0
var ErrNotFound = os.ErrNotExist

type GetOperation struct {
	Storage          // should be initialized
	bucket   []byte
	key      []byte
	ttlPtr     *int
	versionPtr *int64
	required bool
}

func (t *GetOperation) Required() *GetOperation {
	t.required = true
	return t
}

func (t *GetOperation) Bucket(bucket []byte) *GetOperation {
	t.bucket = bucket
	return t
}

func (t *GetOperation) ByKey(formatKey string, args... interface{}) *GetOperation {
	if len(args) > 0 {
		t.key = []byte(fmt.Sprintf(formatKey, args...))
	} else {
		t.key = []byte(formatKey)
	}
	return t
}

func (t *GetOperation) ByRawKey(key []byte) *GetOperation {
	t.key = key
	return t
}

func (t *GetOperation) FetchTtl(ttl *int) *GetOperation {
	t.ttlPtr = ttl
	return t
}

func (t *GetOperation) FetchVersion(ptr *int64) *GetOperation {
	t.versionPtr = ptr
	return t
}

func (t *GetOperation) ToProto(container proto.Message) error {
	content, err := t.GetRaw(t.bucket, t.key, t.ttlPtr, t.versionPtr, t.required)
	if err != nil {
		return err
	}
	if content == nil {
		container.Reset()
		return nil
	} else {
		return proto.Unmarshal(content, container)
	}
}

func (t *GetOperation) ToBinary() ([]byte, error) {
	return t.GetRaw(t.bucket, t.key, t.ttlPtr, t.versionPtr, t.required)
}

func (t *GetOperation) ToString() (string, error) {
	content, err :=  t.GetRaw(t.bucket, t.key, t.ttlPtr, t.versionPtr, t.required)
	if err != nil || content == nil {
		return "", err
	}
	return string(content), nil
}

func (t *GetOperation) ToCounter() (uint64, error) {
	content, err :=  t.GetRaw(t.bucket, t.key, t.ttlPtr, t.versionPtr, t.required)
	if err != nil || len(content) < 8 {
		return 0, err
	}
	return binary.BigEndian.Uint64(content), nil
}

type SetOperation struct {
	Storage            // should be initialized
	bucket     []byte
	key        []byte
	ttlSeconds int
}

func (t *SetOperation) Bucket(bucket []byte) *SetOperation {
	t.bucket = bucket
	return t
}

func (t *SetOperation) ByKey(formatKey string, args... interface{}) *SetOperation {
	if len(args) > 0 {
		t.key = []byte(fmt.Sprintf(formatKey, args...))
	} else {
		t.key = []byte(formatKey)
	}
	return t
}

func (t *SetOperation) ByRawKey(key []byte) *SetOperation {
	t.key = key
	return t
}

func (t *SetOperation) WithTtl(ttlSeconds int) *SetOperation {
	t.ttlSeconds = ttlSeconds
	return t
}

func (t *SetOperation) Binary(value []byte) error {
	return t.Storage.SetRaw(t.bucket, t.key, value, t.ttlSeconds)
}

func (t *SetOperation) String(value string) error {
	return t.Storage.SetRaw(t.bucket, t.key, []byte(value), t.ttlSeconds)
}

func (t *SetOperation) Counter(value uint64) error {
	slice := make([]byte, 8)
	binary.BigEndian.PutUint64(slice, value)
	return t.Storage.SetRaw(t.bucket, t.key, slice, t.ttlSeconds)
}

func (t *SetOperation) Proto(msg proto.Message) error {
	bin, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return t.Storage.SetRaw(t.bucket, t.key, bin, t.ttlSeconds)
}


type CompareAndSetOperation struct {
	Storage            // should be initialized
	bucket     []byte
	key        []byte
	ttlSeconds int
	version    int64
}

func (t *CompareAndSetOperation) Bucket(bucket []byte) *CompareAndSetOperation {
	t.bucket = bucket
	return t
}

func (t *CompareAndSetOperation) ByKey(formatKey string, args... interface{}) *CompareAndSetOperation {
	if len(args) > 0 {
		t.key = []byte(fmt.Sprintf(formatKey, args...))
	} else {
		t.key = []byte(formatKey)
	}
	return t
}

func (t *CompareAndSetOperation) ByRawKey(key []byte) *CompareAndSetOperation {
	t.key = key
	return t
}

func (t *CompareAndSetOperation) WithTtl(ttlSeconds int) *CompareAndSetOperation {
	t.ttlSeconds = ttlSeconds
	return t
}

func (t *CompareAndSetOperation) WithVersion(version int64) *CompareAndSetOperation {
	t.version = version
	return t
}

func (t *CompareAndSetOperation) Binary(value []byte) (bool, error) {
	return t.Storage.CompareAndSetRaw(t.bucket, t.key, value, t.ttlSeconds, t.version)
}

func (t *CompareAndSetOperation) String(value string) (bool, error) {
	return t.Storage.CompareAndSetRaw(t.bucket, t.key, []byte(value), t.ttlSeconds, t.version)
}

func (t *CompareAndSetOperation) Counter(value uint64) (bool, error) {
	slice := make([]byte, 8)
	binary.BigEndian.PutUint64(slice, value)
	return t.Storage.CompareAndSetRaw(t.bucket, t.key, slice, t.ttlSeconds, t.version)
}

func (t *CompareAndSetOperation) Proto(msg proto.Message) (bool, error) {
	bin, err := proto.Marshal(msg)
	if err != nil {
		return false, err
	}
	return t.Storage.CompareAndSetRaw(t.bucket, t.key, bin, t.ttlSeconds, t.version)
}


type IncrementOperation struct {
	Storage            // should be initialized
	bucket     []byte
	key        []byte
	ttlSeconds int
	version    int64
	initial    uint64
	delta      uint64   // should be initialized by 1
}

func (t *IncrementOperation) Bucket(bucket []byte) *IncrementOperation {
	t.bucket = bucket
	return t
}

func (t *IncrementOperation) ByKey(formatKey string, args... interface{}) *IncrementOperation {
	if len(args) > 0 {
		t.key = []byte(fmt.Sprintf(formatKey, args...))
	} else {
		t.key = []byte(formatKey)
	}
	return t
}

func (t *IncrementOperation) ByRawKey(key []byte) *IncrementOperation {
	t.key = key
	return t
}

func (t *IncrementOperation) WithTtl(ttlSeconds int) *IncrementOperation {
	t.ttlSeconds = ttlSeconds
	return t
}

func (t *IncrementOperation) WithInitialValue(initial uint64) *IncrementOperation {
	t.initial = initial
	return t
}

func (t *IncrementOperation) WithDelta(delta uint64) *IncrementOperation {
	t.delta = delta
	return t
}

func (t *IncrementOperation) Do() (prev uint64, err error) {
	err = t.Storage.DoInTransaction(t.bucket, t.key, func(entry *RawEntry) bool {
		counter := t.initial
		if len(entry.Value) >= 8 {
			counter = binary.BigEndian.Uint64(entry.Value)
		}
		prev = counter
		counter += t.delta
		entry.Value = make([]byte, 8)
		binary.BigEndian.PutUint64(entry.Value, counter)
		return true
	})
	return
}

type RemoveOperation struct {
	Storage         // should be initialized
	bucket []byte
	key    []byte
}

func (t *RemoveOperation) Bucket(bucket []byte) *RemoveOperation {
	t.bucket = bucket
	return t
}

func (t *RemoveOperation) ByKey(formatKey string, args... interface{}) *RemoveOperation {
	if len(args) > 0 {
		t.key = []byte(fmt.Sprintf(formatKey, args...))
	} else {
		t.key = []byte(formatKey)
	}
	return t
}

func (t *RemoveOperation) ByRawKey(key []byte) *RemoveOperation {
	t.key = key
	return t
}

func (t *RemoveOperation) Do() error {
	return t.Storage.RemoveRaw(t.bucket, t.key)
}

type EnumerateOperation struct {
	Storage              // should be initialized
	prefixBin []byte
	seekBin   []byte
	batchSize int
	onlyKeys bool
}

func (t *EnumerateOperation) Bucket(bucket []byte) *EnumerateOperation {
	t.prefixBin = bucket
	return t
}

func (t *EnumerateOperation) ByPrefix(formatPrefix string, args... interface{}) *EnumerateOperation {
	if len(args) > 0 {
		t.prefixBin = []byte(fmt.Sprintf(formatPrefix, args...))
	} else {
		t.prefixBin = []byte(formatPrefix)
	}
	if t.seekBin == nil {
		t.seekBin = t.prefixBin
	}
	return t
}

func (t *EnumerateOperation) Seek(formatSeek string, args... interface{}) *EnumerateOperation {
	if len(args) > 0 {
		t.seekBin = []byte(fmt.Sprintf(formatSeek, args...))
	} else {
		t.seekBin = []byte(formatSeek)
	}
	return t
}

func (t *EnumerateOperation) ByRawPrefix(prefix []byte) *EnumerateOperation {
	t.prefixBin = prefix
	return t
}

func (t *EnumerateOperation) WithBatchSize(batchSize int) *EnumerateOperation {
	t.batchSize = batchSize
	return t
}

func (t *EnumerateOperation) OnlyKeys() *EnumerateOperation {
	t.onlyKeys = true
	return t
}

func (t *EnumerateOperation) Do(cb func(*RawEntry) bool) error {
	if t.batchSize <= 0 {
		t.batchSize = 1
	}
	return t.Storage.EnumerateRaw(t.prefixBin, t.seekBin, t.batchSize, t.onlyKeys, cb)
}

func (t *EnumerateOperation) DoProto(factory func() proto.Message, cb func(*ProtoEntry) bool) error {
	if t.batchSize <= 0 {
		t.batchSize = 1
	}
	var marshalErr error
	err := t.Storage.EnumerateRaw(t.prefixBin, t.seekBin, t.batchSize, t.onlyKeys, func(raw *RawEntry) bool {
		item := factory()
		if err := proto.Unmarshal(raw.Value, item); err != nil {
			marshalErr = err
			return false
		}
		pe := ProtoEntry{
			Key: raw.Key,
			Value: item,
			Ttl: raw.Ttl,
			Version: raw.Version,
		}
		return cb(&pe)
	})
	if err == nil {
		err = marshalErr
	}
	return err
}

var StorageManagementClass = reflect.TypeOf((*StorageManagement)(nil)).Elem()
type StorageManagement interface {

	Compact(discardRatio float64) error

	Backup(w io.Writer, since uint64) (uint64, error)

	Restore(r io.Reader) error

	DropAll() error

	DropWithPrefix(prefix []byte) error

}

type RawEntry struct {
	Key []byte
	Value []byte
	Ttl int
	Version int64
}

type ProtoEntry struct {
	Key []byte
	Value proto.Message
	Ttl int
	Version int64
}

var StorageClass = reflect.TypeOf((*Storage)(nil)).Elem()
type Storage interface {
	beans.DisposableBean
	beans.NamedBean

	Get() *GetOperation

	Set() *SetOperation

	// equivalent of i++ operation, always returns previous value
	Increment() *IncrementOperation

	CompareAndSet() *CompareAndSetOperation

	Remove() *RemoveOperation

	Enumerate() *EnumerateOperation

	GetRaw(bucket, key []byte, ttlPtr *int, versionPtr *int64, required bool) ([]byte, error)

	SetRaw(bucket, key, value []byte, ttlSeconds int) error

	CompareAndSetRaw(bucket, key, value []byte, ttlSeconds int, version int64) (bool, error)

	DoInTransaction(prefix, key []byte, cb func(entry *RawEntry) bool) error

	RemoveRaw(bucket, key []byte) error

	EnumerateRaw(bucket, seek []byte, batchSize int, onlyKeys bool, cb func(*RawEntry) bool)  error

	FetchKeysRaw(bucket []byte, batchSize int) ([][]byte, error)
}

var ManagedStorageClass = reflect.TypeOf((*ManagedStorage)(nil)).Elem()
type ManagedStorage interface {
	Storage
	StorageManagement
	
	Instance() interface{}
}

