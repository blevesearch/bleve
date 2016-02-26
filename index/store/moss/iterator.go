//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package moss

import (
	"bytes"

	"github.com/couchbase/moss"
)

type Iterator struct {
	store  *Store
	ss     moss.Snapshot
	iter   moss.Iterator
	prefix []byte
	start  []byte
	end    []byte
	done   bool
	k      []byte
	v      []byte
}

func (x *Iterator) Seek(seekToKey []byte) {
	x.done = true
	x.k = nil
	x.v = nil

	if bytes.Compare(seekToKey, x.start) < 0 {
		seekToKey = x.start
	}

	iter, err := x.ss.StartIterator(seekToKey, x.end, moss.IteratorOptions{})
	if err != nil {
		x.store.Logf("bleve moss StartIterator err: %v", err)
		return
	}

	err = x.iter.Close()
	if err != nil {
		x.store.Logf("bleve moss iterator.Seek err: %v", err)
		return
	}

	x.iter = iter

	x.checkDone()
}

func (x *Iterator) Next() {
	if x.done {
		return
	}

	x.done = true
	x.k = nil
	x.v = nil

	err := x.iter.Next()
	if err != nil {
		return
	}

	x.checkDone()
}

func (x *Iterator) Current() ([]byte, []byte, bool) {
	return x.k, x.v, !x.done
}

func (x *Iterator) Key() []byte {
	if x.done {
		return nil
	}

	return x.k
}

func (x *Iterator) Value() []byte {
	if x.done {
		return nil
	}

	return x.v
}

func (x *Iterator) Valid() bool {
	return !x.done
}

func (x *Iterator) Close() error {
	var err error

	x.ss = nil

	if x.iter != nil {
		err = x.iter.Close()
		x.iter = nil
	}

	x.prefix = nil
	x.done = true
	x.k = nil
	x.v = nil

	return err
}

func (x *Iterator) checkDone() {
	x.done = true
	x.k = nil
	x.v = nil

	k, v, err := x.iter.Current()
	if err != nil {
		return
	}

	if x.prefix != nil && !bytes.HasPrefix(k, x.prefix) {
		return
	}

	x.done = false
	x.k = k
	x.v = v
}
