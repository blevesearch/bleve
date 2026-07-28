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

package search

import (
	"context"
	"fmt"
	"sync/atomic"
)

// termSearchersCounter tracks the running total of leaf term searchers created
// while searching a single index and the limit beyond which construction fails.
type termSearchersCounter struct {
	limit int
	count atomic.Uint64
}

// ContextWithTermSearchersCounter installs a fresh counter for the limit at
// MaxTermSearchersKey, when that limit is positive. It is called once per index
// search (indexImpl.SearchInContext), so the counter is scoped to that index:
// within a single index it spans the whole query tree (all fan-out nodes share
// one counter), but across an IndexAlias each index is counted independently.
// context is returned unchanged and RecordTermSearcher stays a no-op.
func ContextWithTermSearchersCounter(ctx context.Context) context.Context {
	if ctx == nil {
		return ctx
	}
	limit, ok := ctx.Value(MaxTermSearchersKey).(int)
	if !ok || limit <= 0 {
		return ctx
	}
	return context.WithValue(ctx, termSearchersCounterKey,
		&termSearchersCounter{limit: limit})
}

// RecordTermSearcher counts one leaf term searcher, returning an error once the
// running total exceeds the limit. It is a no-op when no counter was installed.
func RecordTermSearcher(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	c, ok := ctx.Value(termSearchersCounterKey).(*termSearchersCounter)
	if !ok || c == nil {
		return nil
	}
	if n := c.count.Add(1); n > uint64(c.limit) {
		return fmt.Errorf("TooManyTermSearchers [%d > bleveMaxTerms,"+
			" which is set to %d]", n, c.limit)
	}
	return nil
}
