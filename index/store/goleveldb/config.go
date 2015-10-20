package goleveldb

import (
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func applyConfig(o *opt.Options, config map[string]interface{}) (*opt.Options, error) {

	ro, ok := config["read_only"].(bool)
	if ok {
		o.ReadOnly = ro
	}

	cim, ok := config["create_if_missing"].(bool)
	if ok {
		o.ErrorIfMissing = !cim
	}

	eie, ok := config["error_if_exists"].(bool)
	if ok {
		o.ErrorIfExist = eie
	}

	wbs, ok := config["write_buffer_size"].(float64)
	if ok {
		o.WriteBuffer = int(wbs)
	}

	bs, ok := config["block_size"].(float64)
	if ok {
		o.BlockSize = int(bs)
	}

	bri, ok := config["block_restart_interval"].(float64)
	if ok {
		o.BlockRestartInterval = int(bri)
	}

	lcc, ok := config["lru_cache_capacity"].(float64)
	if ok {
		o.BlockCacheCapacity = int(lcc)
	}

	bfbpk, ok := config["bloom_filter_bits_per_key"].(float64)
	if ok {
		bf := filter.NewBloomFilter(int(bfbpk))
		o.Filter = bf
	}

	return o, nil
}
