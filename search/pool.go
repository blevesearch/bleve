//  Copyright (c) 2014 Couchbase, Inc.
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

package search

import (
	"reflect"
)

var reflectStaticSizeDocumentMatchPool int

func init() {
	var dmp DocumentMatchPool
	reflectStaticSizeDocumentMatchPool = int(reflect.TypeOf(dmp).Size())
}

// DocumentMatchPoolTooSmall is a callback function that can be executed
// when the DocumentMatchPool does not have sufficient capacity
// By default we just perform just-in-time allocation, but you could log
// a message, or panic, etc.
type DocumentMatchPoolTooSmall func(p *DocumentMatchPool) *DocumentMatch

// DocumentMatchPool reuses DocumentMatch instances and allocates reserved
// matches in progressively larger blocks. Each pool belongs to one goroutine.
type DocumentMatchPool struct {
	avail         DocumentMatchCollection
	remaining     int
	nextBlockSize int
	sortSize      int
	TooSmall      DocumentMatchPoolTooSmall
}

func defaultDocumentMatchPoolTooSmall(p *DocumentMatchPool) *DocumentMatch {
	return &DocumentMatch{}
}

// NewDocumentMatchPool reserves size matches for use before invoking TooSmall.
func NewDocumentMatchPool(size, sortSize int) *DocumentMatchPool {
	return &DocumentMatchPool{
		remaining:     size,
		nextBlockSize: 32,
		sortSize:      sortSize,
		TooSmall:      defaultDocumentMatchPoolTooSmall,
	}
}

// Get returns an available DocumentMatch from the pool.
func (p *DocumentMatchPool) Get() *DocumentMatch {
	if len(p.avail) == 0 {
		if p.remaining == 0 {
			return p.TooSmall(p)
		}
		p.allocateBlock()
	}
	last := len(p.avail) - 1
	match := p.avail[last]
	p.avail = p.avail[:last]
	return match
}

func (p *DocumentMatchPool) allocateBlock() {
	// Search windows can be much larger than the matches they find.
	size := min(p.remaining, p.nextBlockSize)
	p.remaining -= size
	p.nextBlockSize = size * 2

	if cap(p.avail) < size {
		p.avail = make(DocumentMatchCollection, size)
	} else {
		p.avail = p.avail[:size]
	}

	matches := make([]DocumentMatch, size)
	sorts := make([]string, size*p.sortSize)
	for i := range matches {
		start := i * p.sortSize
		p.avail[i] = &matches[i]
		p.avail[i].Sort = sorts[start:start]
	}
}

// Put returns a DocumentMatch to the pool
func (p *DocumentMatchPool) Put(d *DocumentMatch) {
	if d == nil {
		return
	}
	// reset DocumentMatch before returning it to available pool
	d.Reset()
	p.avail = append(p.avail, d)
}
