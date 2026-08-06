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
	"testing"
)

func TestRecordTermSearcher(t *testing.T) {
	// No limit set: RecordTermSearcher must always be a no-op.
	t.Run("no limit set", func(t *testing.T) {
		ctx := ContextWithTermSearchersCounter(context.Background())
		for i := 0; i < 1000; i++ {
			if err := RecordTermSearcher(ctx); err != nil {
				t.Fatalf("unexpected error with no limit at i=%d: %v", i, err)
			}
		}
	})

	// Non-positive limit disables the check.
	for _, limit := range []int{0, -5} {
		t.Run("disabled limit", func(t *testing.T) {
			ctx := context.WithValue(context.Background(), MaxTermSearchersKey, limit)
			ctx = ContextWithTermSearchersCounter(ctx)
			for i := 0; i < 100; i++ {
				if err := RecordTermSearcher(ctx); err != nil {
					t.Fatalf("limit %d should disable check, got err at i=%d: %v",
						limit, i, err)
				}
			}
		})
	}

	// Positive limit: exactly `limit` leaf term searchers are allowed; the
	// one that pushes the total over the limit fails.
	t.Run("limit enforced", func(t *testing.T) {
		const limit = 3
		ctx := context.WithValue(context.Background(), MaxTermSearchersKey, limit)
		ctx = ContextWithTermSearchersCounter(ctx)
		for i := 1; i <= limit; i++ {
			if err := RecordTermSearcher(ctx); err != nil {
				t.Fatalf("term searcher %d/%d should be allowed, got: %v", i, limit, err)
			}
		}
		if err := RecordTermSearcher(ctx); err == nil {
			t.Fatalf("term searcher %d should exceed limit %d, got nil error",
				limit+1, limit)
		}
	})

	// A nil context is tolerated.
	t.Run("nil context", func(t *testing.T) {
		if err := RecordTermSearcher(nil); err != nil {
			t.Fatalf("nil ctx should be a no-op, got: %v", err)
		}
		//nolint:staticcheck // deliberately exercising nil-ctx tolerance
		if got := ContextWithTermSearchersCounter(nil); got != nil {
			t.Fatalf("nil ctx should be returned unchanged")
		}
	})
}
