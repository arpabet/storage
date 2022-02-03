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

package badgerstorage

import (
	"go.uber.org/zap"
	"os"
	"errors"
	"time"
)

var (

	ValueLogMaxEntries = uint32(1024 * 1024 * 1024)
	KeyRotationDuration = time.Hour * 24 * 7

	KeySize = 32

	DefaultDirPerm  = os.FileMode(0775)

	MaxPendingWrites = 4096

	ErrInvalidKeySize = errors.New("invalid key size")
	ErrCanceled = errors.New("operation was canceled")
	ErrDatabaseExist = errors.New("database exist")
	ErrDatabaseNotExist = errors.New("database not exist")
	ErrItemNotExist = errors.New("item not exist")
)

type BadgerAction uint8

const (

	DeleteIfExist BadgerAction = iota
	CreateIfNotExist

)

type BadgerConfig struct {
	DataDir    string
	Action     BadgerAction
	StorageKey []byte      // optional
	UseZSTD     bool
	TruncateDB  bool
	Debug       bool
	Log         *zap.Logger  // optional
	DirPerm    os.FileMode
}

