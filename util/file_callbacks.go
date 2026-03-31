//  Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	index "github.com/blevesearch/bleve_index_api"
)

// This file provides a mechanism for users of bleve to provide callbacks
// that can process data before it is written to disk, and after it is read
// from disk.  This can be used for things like encryption, compression, etc.

// The user is responsible for ensuring that the writer and reader callbacks
// are compatible with each other, and that any state needed by the callbacks
// is managed appropriately.  For example, if the writer callback uses a
// unique key or nonce per write, the reader callback must be able to
// determine the correct key or nonce to use for each read.

// The callbacks are identified by an id string, which is returned by the
// WriterHook. The same id string is passed to the ReaderHook when creating a reader.
// This allows the reader to determine which callback to use for a given file.

// Support for identifying all callbacks used by a given index and to remove
// selected callbacks associated with ids is provided via index.WriterIdsInUse()
// and index.DropWriterIds().

// FileWriter and FileReader are wrappers around the callback functions provided
// by the user. They provide a convenient way to apply the callbacks to data
// being written to or read from a file. They also store the id the callbacks,
// which can be useful for managing state across multiple reads and writes.
type FileWriter struct {
	id        string
	processor func(data []byte) []byte
}

func NewFileWriter(context []byte) (*FileWriter, error) {
	rv := &FileWriter{}

	if index.WriterHook != nil {
		var err error
		rv.id, rv.processor, err = index.WriterHook(context)
		if err != nil {
			return nil, err
		}
	}

	return rv, nil
}

func (w *FileWriter) Process(data []byte) []byte {
	if w.processor != nil {
		return w.processor(data)
	}
	return data
}

func (w *FileWriter) Id() string {
	return w.id
}

type FileReader struct {
	id        string
	processor func(data []byte) ([]byte, error)
}

func NewFileReader(id string, context []byte) (*FileReader, error) {
	rv := &FileReader{
		id: id,
	}

	if index.ReaderHook != nil {
		var err error
		rv.processor, err = index.ReaderHook(id, context)
		if err != nil {
			return nil, err
		}
	}

	return rv, nil
}

func (r *FileReader) Process(data []byte) ([]byte, error) {
	if r.processor != nil {
		return r.processor(data)
	}
	return data, nil
}

func (r *FileReader) Id() string {
	return r.id
}
