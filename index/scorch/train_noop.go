//go:build !vectors
// +build !vectors

package scorch

import (
	"fmt"

	index "github.com/blevesearch/bleve_index_api"
	bolt "go.etcd.io/bbolt"
)

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
