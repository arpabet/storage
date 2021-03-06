/**
  Copyright (c) 2022 Arpabet, LLC. All rights reserved.
*/

package storage

import (
	"encoding/binary"
	"go.arpabet.com/beans"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"reflect"
)

const NoTTL = 0
var ErrNotFound = os.ErrNotExist
var DefaultBatchSize = 100

type GetOperation struct {
	Storage          // should be initialized
	key      []byte
	required bool
}

func (t *GetOperation) Required() *GetOperation {
	t.required = true
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

func (t *GetOperation) ToProto(container proto.Message) error {
	var ttl int
	var version int64
	value, err := t.GetRaw(t.key, &ttl, &version, t.required)
	if err != nil || value == nil {
		return err
	}
	return proto.Unmarshal(value, container)
}

func (t *GetOperation) ToBinary() ([]byte, error) {
	var ttl int
	var version int64
	return t.GetRaw(t.key, &ttl, &version, t.required)
}

func (t *GetOperation) ToString() (string, error) {
	var ttl int
	var version int64
	content, err :=  t.GetRaw(t.key, &ttl, &version, t.required)
	if err != nil || content == nil {
		return "", err
	}
	return string(content), nil
}

func (t *GetOperation) ToCounter() (uint64, error) {
	var ttl int
	var version int64
	content, err :=  t.GetRaw(t.key, &ttl, &version, t.required)
	if err != nil || len(content) < 8 {
		return 0, err
	}
	return binary.BigEndian.Uint64(content), nil
}

func (t *GetOperation) ToEntry() (entry RawEntry, err error) {
	entry.Key = t.key
	entry.Value, err = t.GetRaw(t.key, &entry.Ttl, &entry.Version, t.required)
	return
}

func (t *GetOperation) ToProtoEntry(factory func() proto.Message) (entry ProtoEntry, err error) {
	entry.Key = t.key
	var value []byte
	if value, err = t.GetRaw(t.key, &entry.Ttl, &entry.Version, t.required); err != nil {
		return
	}
	if value != nil {
		entry.Value = factory()
		err = proto.Unmarshal(value, entry.Value)
	}
	return
}

type SetOperation struct {
	Storage            // should be initialized
	key        []byte
	ttlSeconds int
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
	return t.Storage.SetRaw(t.key, value, t.ttlSeconds)
}

func (t *SetOperation) String(value string) error {
	return t.Storage.SetRaw(t.key, []byte(value), t.ttlSeconds)
}

func (t *SetOperation) Counter(value uint64) error {
	slice := make([]byte, 8)
	binary.BigEndian.PutUint64(slice, value)
	return t.Storage.SetRaw(t.key, slice, t.ttlSeconds)
}

func (t *SetOperation) Proto(msg proto.Message) error {
	bin, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return t.Storage.SetRaw(t.key, bin, t.ttlSeconds)
}


type CompareAndSetOperation struct {
	Storage            // should be initialized
	key        []byte
	ttlSeconds int
	version    int64
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
	return t.Storage.CompareAndSetRaw(t.key, value, t.ttlSeconds, t.version)
}

func (t *CompareAndSetOperation) String(value string) (bool, error) {
	return t.Storage.CompareAndSetRaw(t.key, []byte(value), t.ttlSeconds, t.version)
}

func (t *CompareAndSetOperation) Counter(value uint64) (bool, error) {
	slice := make([]byte, 8)
	binary.BigEndian.PutUint64(slice, value)
	return t.Storage.CompareAndSetRaw(t.key, slice, t.ttlSeconds, t.version)
}

func (t *CompareAndSetOperation) Proto(msg proto.Message) (bool, error) {
	bin, err := proto.Marshal(msg)
	if err != nil {
		return false, err
	}
	return t.Storage.CompareAndSetRaw(t.key, bin, t.ttlSeconds, t.version)
}

type IncrementOperation struct {
	Storage            // should be initialized
	key        []byte
	ttlSeconds int
	version    int64
	Initial    uint64
	Delta      uint64   // should be initialized by 1
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
	t.Initial = initial
	return t
}

func (t *IncrementOperation) WithDelta(delta uint64) *IncrementOperation {
	t.Delta = delta
	return t
}

func (t *IncrementOperation) Do() (prev uint64, err error) {
	err = t.Storage.DoInTransaction(t.key, func(entry *RawEntry) bool {
		counter := t.Initial
		if len(entry.Value) >= 8 {
			counter = binary.BigEndian.Uint64(entry.Value)
		}
		prev = counter
		counter += t.Delta
		entry.Value = make([]byte, 8)
		binary.BigEndian.PutUint64(entry.Value, counter)
		return true
	})
	return
}

type RemoveOperation struct {
	Storage         // should be initialized
	key    []byte
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
	return t.Storage.RemoveRaw(t.key)
}

type EnumerateOperation struct {
	Storage              // should be initialized
	prefixBin []byte
	seekBin   []byte
	batchSize int
	onlyKeys bool
}

func (t *EnumerateOperation) ByPrefix(formatPrefix string, args... interface{}) *EnumerateOperation {
	if len(args) > 0 {
		t.prefixBin = []byte(fmt.Sprintf(formatPrefix, args...))
	} else {
		t.prefixBin = []byte(formatPrefix)
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
		t.batchSize = DefaultBatchSize
	}
	if t.seekBin == nil {
		t.seekBin = t.prefixBin
	}
	return t.Storage.EnumerateRaw(t.prefixBin, t.seekBin, t.batchSize, t.onlyKeys, cb)
}

func (t *EnumerateOperation) DoProto(factory func() proto.Message, cb func(*ProtoEntry) bool) error {
	if t.batchSize <= 0 {
		t.batchSize = DefaultBatchSize
	}
	if t.seekBin == nil {
		t.seekBin = t.prefixBin
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

	GetRaw(key []byte, ttlPtr *int, versionPtr *int64, required bool) ([]byte, error)

	SetRaw(key, value []byte, ttlSeconds int) error

	CompareAndSetRaw(key, value []byte, ttlSeconds int, version int64) (bool, error)

	DoInTransaction(key []byte, cb func(entry *RawEntry) bool) error

	RemoveRaw(key []byte) error

	EnumerateRaw(prefix, seek []byte, batchSize int, onlyKeys bool, cb func(*RawEntry) bool)  error

}

var ManagedStorageClass = reflect.TypeOf((*ManagedStorage)(nil)).Elem()
type ManagedStorage interface {
	Storage
	StorageManagement
	
	Instance() interface{}
}

