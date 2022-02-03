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
	"fmt"
	"github.com/dgraph-io/badger/v2"
	"go.uber.org/zap"
)

type zapLoggerAdapter struct {
	log   *zap.Logger
	debug  bool
}

func (t* zapLoggerAdapter) Errorf(format string, args ...interface{}) {
	t.log.Error("Badger", zap.String("log", fmt.Sprintf(format, args...)))
}

func (t* zapLoggerAdapter) Warningf(format string, args ...interface{}) {
	t.log.Warn("Badger", zap.String("log", fmt.Sprintf(format, args...)))
}

func (t* zapLoggerAdapter) Infof(format string, args ...interface{}) {
	t.log.Info("Badger", zap.String("log", fmt.Sprintf(format, args...)))
}

func (t* zapLoggerAdapter) Debugf(format string, args ...interface{}) {
	if t.debug {
		t.log.Debug("Badger", zap.String("log", fmt.Sprintf(format, args...)))
	}
}

func NewZapLogger(log *zap.Logger, debug bool) badger.Logger {
	return &zapLoggerAdapter{ log: log, debug: debug }
}