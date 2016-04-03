//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package firestorm

import (
	"io/ioutil"
	"log"

	"github.com/blevesearch/bleve/index/store"
)

type KVVisitor func(key, val []byte) (bool, error)

func visitPrefix(reader store.KVReader, prefix []byte, visitor KVVisitor) (err error) {
	start := prefix
	if start == nil {
		start = []byte{}
	}
	it := reader.PrefixIterator(start)
	defer func() {
		if cerr := it.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	k, v, valid := it.Current()
	for valid {
		var cont bool
		cont, err = visitor(k, v)
		if err != nil {
			// visitor encountered an error, stop and return it
			return
		}
		if !cont {
			// vistor has requested we stop iteration, return nil
			return
		}
		it.Next()
		k, v, valid = it.Current()
	}
	return
}

func visitRange(reader store.KVReader, start, end []byte, visitor KVVisitor) (err error) {
	it := reader.RangeIterator(start, end)
	defer func() {
		if cerr := it.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	k, v, valid := it.Current()
	for valid {
		var cont bool
		cont, err = visitor(k, v)
		if err != nil {
			// visitor encountered an error, stop and return it
			return
		}
		if !cont {
			// vistor has requested we stop iteration, return nil
			return
		}
		it.Next()
		k, v, valid = it.Current()
	}
	return
}

type DocNumberList []uint64

func (l DocNumberList) Len() int           { return len(l) }
func (l DocNumberList) Less(i, j int) bool { return l[i] > l[j] }
func (l DocNumberList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

// HighestValid returns the highest valid doc number
// from a *SORTED* DocNumberList
// if no doc number in the list is valid, then 0
func (l DocNumberList) HighestValid(maxRead uint64) uint64 {
	for _, dn := range l {
		if dn <= maxRead {
			return dn
		}
	}
	return 0
}

var logger = log.New(ioutil.Discard, "bleve.index.firestorm ", 0)

// SetLog sets the logger used for logging
// by default log messages are sent to ioutil.Discard
func SetLog(l *log.Logger) {
	logger = l
}
