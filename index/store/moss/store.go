//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

// Package moss provides a KVStore implementation based on the
// github.com/couchbaselabs/moss library.

package moss

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/couchbase/moss"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
)

const Name = "moss"

type Store struct {
	m       sync.Mutex
	ms      moss.Collection
	mo      store.MergeOperator
	llstore store.KVStore
}

func New(mo store.MergeOperator, config map[string]interface{}) (
	store.KVStore, error) {
	return NewEx(mo, config, moss.CollectionOptions{})
}

func NewEx(mo store.MergeOperator, config map[string]interface{},
	options moss.CollectionOptions) (store.KVStore, error) {
	debug := moss.DefaultCollectionOptions.Debug
	v, ok := config["mossDebug"]
	if ok {
		debugF, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("moss store,"+
				" could not parse config[mossDebug]: %v", v)
		}

		debug = int(debugF)
	}

	minMergePercentage := moss.DefaultCollectionOptions.MinMergePercentage
	v, ok = config["mossMinMergePercentage"]
	if ok {
		minMergePercentage, ok = v.(float64)
		if !ok {
			return nil, fmt.Errorf("moss store,"+
				" could not parse config[mossMinMergePercentage]: %v", v)
		}
	}

	maxPreMergerBatches := moss.DefaultCollectionOptions.MaxPreMergerBatches
	v, ok = config["mossMaxPreMergerBatches"]
	if ok {
		maxPreMergerBatchesF, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("moss store,"+
				" could not parse config[mossMaxPreMergerBatches]: %v", v)
		}

		maxPreMergerBatches = int(maxPreMergerBatchesF)
	}

	mossLowerLevelStoreName := ""
	v, ok = config["mossLowerLevelStoreName"]
	if ok {
		mossLowerLevelStoreName, ok = v.(string)
		if !ok {
			return nil, fmt.Errorf("moss store,"+
				" could not parse config[mossLowerLevelStoreName]: %v", v)
		}
	}

	mossLowerLevelMaxBatchSize := uint64(0)
	v, ok = config["mossLowerLevelMaxBatchSize"]
	if ok {
		mossLowerLevelMaxBatchSizeF, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("moss store,"+
				" could not parse config[mossLowerLevelMaxBatchSize]: %v", v)
		}

		mossLowerLevelMaxBatchSize = uint64(mossLowerLevelMaxBatchSizeF)
	}

	// --------------------------------------------------

	if options.MergeOperator == nil {
		options.MergeOperator = mo
	}

	if options.MinMergePercentage <= 0 {
		options.MinMergePercentage = minMergePercentage
	}

	if options.MaxPreMergerBatches <= 0 {
		options.MaxPreMergerBatches = maxPreMergerBatches
	}

	if options.Debug <= 0 {
		options.Debug = debug
	}

	if options.Log == nil {
		options.Log = func(format string, a ...interface{}) {}
	}

	var llStore store.KVStore
	if options.LowerLevelInit == nil &&
		options.LowerLevelUpdate == nil &&
		mossLowerLevelStoreName != "" {
		mossLowerLevelStoreConfig := map[string]interface{}{}

		v, ok := config["mossLowerLevelStoreConfig"]
		if ok {
			mossLowerLevelStoreConfig, ok = v.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("moss store, initLowerLevelStore,"+
					" could parse mossLowerLevelStoreConfig: %v", v)
			}
		}

		lowerLevelInit, lowerLevelUpdate, lowerLevelStore, err :=
			initLowerLevelStore(mo, config,
				mossLowerLevelStoreName,
				mossLowerLevelStoreConfig,
				mossLowerLevelMaxBatchSize,
				options.Log)
		if err != nil {
			return nil, err
		}

		options.LowerLevelInit = lowerLevelInit
		options.LowerLevelUpdate = lowerLevelUpdate
		llStore = lowerLevelStore
	}

	// --------------------------------------------------

	ms, err := moss.NewCollection(options)
	if err != nil {
		return nil, err
	}
	err = ms.Start()
	if err != nil {
		return nil, err
	}
	rv := Store{
		ms:      ms,
		mo:      mo,
		llstore: llStore,
	}
	return &rv, nil
}

func (s *Store) Close() error {
	return s.ms.Close()
}

func (s *Store) Reader() (store.KVReader, error) {
	ss, err := s.ms.Snapshot()
	if err != nil {
		return nil, err
	}
	return &Reader{ss: ss}, nil
}

func (s *Store) Writer() (store.KVWriter, error) {
	return &Writer{s: s}, nil
}

func (s *Store) Logf(fmt string, args ...interface{}) {
	options := s.ms.Options()
	if options.Log != nil {
		options.Log(fmt, args...)
	}
}

func (s *Store) Stats() json.Marshaler {
	rv := stats{
		s: s,
	}
	if llstore, ok := s.llstore.(store.KVStoreStats); ok {
		rv.llstats = llstore.Stats()
	}
	return &rv
}

func init() {
	registry.RegisterKVStore(Name, New)
}
