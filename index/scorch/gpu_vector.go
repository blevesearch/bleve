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

//go:build vectors
// +build vectors

package scorch

import (
	index "github.com/blevesearch/bleve_index_api"
)

// setGPUErrorCallbacks forwards GPU error callbacks from the user-supplied
// config into the segment config so that zapx can invoke them on GPU failures.
func setGPUErrorCallbacks(config, segmentConfig map[string]interface{}) {
	if segmentConfig == nil {
		return
	}

	if cb, ok := config[index.CPUToGPUCloneErrorKey]; ok {
		segmentConfig[index.CPUToGPUCloneErrorKey] = cb
	}
	if cb, ok := config[index.GPUErrorKey]; ok {
		segmentConfig[index.GPUErrorKey] = cb
	}
}
