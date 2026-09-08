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

package geov2

import (
	"sync"

	"github.com/blevesearch/geo/s2"
)

// Bounds on the scratch buffers handed to shape decoding, matching the sizes the
// non-v2 geo shape searcher uses (search.MaxGeoBufPoolSize and
// search.MinGeoBufPoolSize). They are repeated here rather than imported so that
// this package does not have to depend on the search package, which would put a
// dependency on search behind the document package.
const (
	maxGeoBufPoolSize = 24 * 1024
	minGeoBufPoolSize = 24
)

// A GeoBufferPool holds the scratch buffers that shape decoding reads vertices
// through. The buffers inside one are allocated on demand and then kept, so a
// pool is only worth having if it is used more than a handful of times: building
// a fresh one for a single shape costs more than decoding that shape without a
// pool at all.
//
// These are therefore kept process-wide rather than built per query or per
// segment, so that the buffers survive across both and the cost of allocating
// them is paid a few times in total. A pool is not safe for concurrent use, so a
// caller takes one for its own use and returns it when done.
var geoBufferPools = sync.Pool{
	New: func() interface{} {
		return s2.NewGeoBufferPool(maxGeoBufPoolSize, minGeoBufPoolSize)
	},
}

// shapeDecodeBuffers hands out a pool for decoding stored shapes, along with the
// function that returns it. It is deliberately called at the point of the first
// decode rather than up front: a query whose cell coverage settles every
// document never decodes a shape, and should not pay for a pool.
func shapeDecodeBuffers() (*s2.GeoBufferPool, func()) {
	pool := geoBufferPools.Get().(*s2.GeoBufferPool)

	return pool, func() { geoBufferPools.Put(pool) }
}
