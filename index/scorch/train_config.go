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

package scorch

// IndexTrainedWithFastMerge is the store config key used to track whether an
// index was configured to train for fast merge. It's declared without a
// "vectors" build tag - unlike the rest of the training implementation in
// train_vector.go/train_noop.go - because external packages need to
// reference this key regardless of whether they were built with that tag.
const IndexTrainedWithFastMerge = "vector_index_fast_merge"
