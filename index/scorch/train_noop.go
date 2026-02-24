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

//go:build !vectors
// +build !vectors

package scorch

import (
	"fmt"

	index "github.com/blevesearch/bleve_index_api"
	bolt "go.etcd.io/bbolt"
)

func initTrainer(s *Scorch, config map[string]interface{}) *noopTrainer {
	return &noopTrainer{}
}

type noopTrainer struct {
}

func (t *noopTrainer) trainLoop() {}

func (t *noopTrainer) train(batch *index.Batch) error {
	return fmt.Errorf("training is not supported with this build")
}

func (t *noopTrainer) loadTrainedData(bucket *bolt.Bucket) error {
	// noop
	return nil
}

func (t *noopTrainer) getInternal(key []byte) ([]byte, error) {
	return nil, nil
}
