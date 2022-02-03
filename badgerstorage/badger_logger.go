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
	"github.com/dgraph-io/badger/v2"
	"log"
)

type loggerAdapter struct {
	debug  bool
}

func (t* loggerAdapter) Errorf(format string, args ...interface{}) {
	log.Printf("ERROR " + format, args...)
}

func (t* loggerAdapter) Warningf(format string, args ...interface{}) {
	log.Printf("WARN " + format, args...)
}

func (t* loggerAdapter) Infof(format string, args ...interface{}) {
	log.Printf("INFO " + format, args...)
}

func (t* loggerAdapter) Debugf(format string, args ...interface{}) {
	if t.debug {
		log.Printf("DEBUG " + format, args...)
	}
}

func NewLogger(debug  bool) badger.Logger {
	return &loggerAdapter{debug: debug}
}