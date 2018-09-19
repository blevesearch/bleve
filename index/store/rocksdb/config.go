package rocksdb

import (
	"strings"
	"github.com/tecbot/gorocksdb"
)

func applyConfig(o *gorocksdb.Options, config map[string]interface{}) (
	*gorocksdb.Options, error) {

	cim, ok := config["create_if_missing"].(bool)
	if ok {
		o.SetCreateIfMissing(cim)
	}

	eie, ok := config["error_if_exists"].(bool)
	if ok {
		o.SetErrorIfExists(eie)
	}

	pc, ok := config["paranoid_checks"].(bool)
	if ok {
		o.SetParanoidChecks(pc)
	}

	ill, ok := config["info_log_level"].(float64)
	if ok {
		o.SetInfoLogLevel(gorocksdb.InfoLogLevel(int(ill)))
	}

	tt, ok := config["total_threads"].(float64)
	if ok {
		o.IncreaseParallelism(int(tt))
	}

	ofpl, ok := config["optimize_for_point_lookup"].(float64)
	if ok {
		o.OptimizeForPointLookup(uint64(ofpl))
	}

	olsc, ok := config["optimize_level_style_compaction"].(float64)
	if ok {
		o.OptimizeLevelStyleCompaction(uint64(olsc))
	}

	ousc, ok := config["optimize_universal_style_compaction"].(float64)
	if ok {
		o.OptimizeUniversalStyleCompaction(uint64(ousc))
	}

	wbs, ok := config["write_buffer_size"].(float64)
	if ok {
		o.SetWriteBufferSize(int(wbs))
	}

	mwbn, ok := config["max_write_buffer_number"].(float64)
	if ok {
		o.SetMaxWriteBufferNumber(int(mwbn))
	}

	mwbntm, ok := config["min_write_buffer_number_to_merge"].(float64)
	if ok {
		o.SetMinWriteBufferNumberToMerge(int(mwbntm))
	}

	mof, ok := config["max_open_files"].(float64)
	if ok {
		o.SetMaxOpenFiles(int(mof))
	}

	c, ok := config["compression"].(float64)
	if ok {
		o.SetCompression(gorocksdb.CompressionType(int(c)))
	}

	mltc, ok := config["min_level_to_compress"].(float64)
	if ok {
		o.SetMinLevelToCompress(int(mltc))
	}

	nl, ok := config["num_levels"].(float64)
	if ok {
		o.SetNumLevels(int(nl))
	}

	lfnct, ok := config["level0_file_num_compaction_trigger"].(float64)
	if ok {
		o.SetLevel0FileNumCompactionTrigger(int(lfnct))
	}

	lswt, ok := config["level0_slowdown_writes_trigger"].(float64)
	if ok {
		o.SetLevel0SlowdownWritesTrigger(int(lswt))
	}

	lstopwt, ok := config["level0_stop_writes_trigger"].(float64)
	if ok {
		o.SetLevel0StopWritesTrigger(int(lstopwt))
	}

	mmcl, ok := config["max_mem_compaction_level"].(float64)
	if ok {
		o.SetMaxMemCompactionLevel(int(mmcl))
	}

	tfsb, ok := config["target_file_size_base"].(float64)
	if ok {
		o.SetTargetFileSizeBase(uint64(tfsb))
	}

	tfsm, ok := config["target_file_size_multiplier"].(float64)
	if ok {
		o.SetTargetFileSizeMultiplier(int(tfsm))
	}

	mbflb, ok := config["max_bytes_for_level_base"].(float64)
	if ok {
		o.SetMaxBytesForLevelBase(uint64(mbflb))
	}

	mbflm, ok := config["max_bytes_for_level_multiplier"].(float64)
	if ok {
		o.SetMaxBytesForLevelMultiplier(mbflm)
	}

	uf, ok := config["use_fsync"].(bool)
	if ok {
		o.SetUseFsync(uf)
	}

	dofpm, ok := config["delete_obsolete_files_period_micros"].(float64)
	if ok {
		o.SetDeleteObsoleteFilesPeriodMicros(uint64(dofpm))
	}

	mbc, ok := config["max_background_compactions"].(float64)
	if ok {
		o.SetMaxBackgroundCompactions(int(mbc))
	}

	mbf, ok := config["max_background_flushes"].(float64)
	if ok {
		o.SetMaxBackgroundFlushes(int(mbf))
	}

	mlfs, ok := config["max_log_file_size"].(float64)
	if ok {
		o.SetMaxLogFileSize(int(mlfs))
	}

	lfttr, ok := config["log_file_time_to_roll"].(float64)
	if ok {
		o.SetLogFileTimeToRoll(int(lfttr))
	}

	klfn, ok := config["keep_log_file_num"].(float64)
	if ok {
		o.SetKeepLogFileNum(int(klfn))
	}

	hrl, ok := config["hard_rate_limit"].(float64)
	if ok {
		o.SetHardRateLimit(hrl)
	}

	rldmm, ok := config["rate_limit_delay_max_millisecond"].(float64)
	if ok {
		o.SetRateLimitDelayMaxMilliseconds(uint(rldmm))
	}

	mmfs, ok := config["max_manifest_file_size"].(float64)
	if ok {
		o.SetMaxManifestFileSize(uint64(mmfs))
	}

	tcnsb, ok := config["table_cache_numshardbits"].(float64)
	if ok {
		o.SetTableCacheNumshardbits(int(tcnsb))
	}

	tcrscl, ok := config["table_cache_remove_scan_count_limit"].(float64)
	if ok {
		o.SetTableCacheRemoveScanCountLimit(int(tcrscl))
	}

	abs, ok := config["arena_block_size"].(float64)
	if ok {
		o.SetArenaBlockSize(int(abs))
	}

	dac, ok := config["disable_auto_compactions"].(bool)
	if ok {
		o.SetDisableAutoCompactions(dac)
	}

	wts, ok := config["WAL_ttl_seconds"].(float64)
	if ok {
		o.SetWALTtlSeconds(uint64(wts))
	}

	wslm, ok := config["WAL_size_limit_MB"].(float64)
	if ok {
		o.SetWalSizeLimitMb(uint64(wslm))
	}

	mps, ok := config["manifest_preallocation_size"].(float64)
	if ok {
		o.SetManifestPreallocationSize(int(mps))
	}

	prkwf, ok := config["purge_redundant_kvs_while_flush"].(bool)
	if ok {
		o.SetPurgeRedundantKvsWhileFlush(prkwf)
	}

	amr, ok := config["allow_mmap_reads"].(bool)
	if ok {
		o.SetAllowMmapReads(amr)
	}

	amw, ok := config["allow_mmap_writes"].(bool)
	if ok {
		o.SetAllowMmapWrites(amw)
	}

	sleor, ok := config["skip_log_error_on_recovery"].(bool)
	if ok {
		o.SetSkipLogErrorOnRecovery(sleor)
	}

	sdps, ok := config["stats_dump_period_sec"].(float64)
	if ok {
		o.SetStatsDumpPeriodSec(uint(sdps))
	}

	aroo, ok := config["advise_random_on_open"].(bool)
	if ok {
		o.SetAdviseRandomOnOpen(aroo)
	}

	ahocs, ok := config["access_hint_on_compaction_start"].(float64)
	if ok {
		o.SetAccessHintOnCompactionStart(gorocksdb.CompactionAccessPattern(uint(ahocs)))
	}

	uam, ok := config["use_adaptive_mutex"].(bool)
	if ok {
		o.SetUseAdaptiveMutex(uam)
	}

	bps, ok := config["bytes_per_sync"].(float64)
	if ok {
		o.SetBytesPerSync(uint64(bps))
	}

	cs, ok := config["compaction_style"].(float64)
	if ok {
		o.SetCompactionStyle(gorocksdb.CompactionStyle(uint(cs)))
	}

	mssii, ok := config["max_sequential_skip_in_iterations"].(float64)
	if ok {
		o.SetMaxSequentialSkipInIterations(uint64(mssii))
	}

	ius, ok := config["inplace_update_support"].(bool)
	if ok {
		o.SetInplaceUpdateSupport(ius)
	}

	iunl, ok := config["inplace_update_num_locks"].(float64)
	if ok {
		o.SetInplaceUpdateNumLocks(int(iunl))
	}

	es, ok := config["enable_statistics"].(bool)
	if ok && es {
		o.EnableStatistics()
	}

	pfbl, ok := config["prepare_for_bulk_load"].(bool)
	if ok && pfbl {
		o.PrepareForBulkLoad()
	}

	comlp, ok := config["compression_per_level"].([]interface{})
	if ok {
		var comValue []gorocksdb.CompressionType
	LOOP:
		for _, v := range comlp {
			var val string
			switch v.(type) {
			case string:
				val = v.(string)
				val = strings.TrimSpace(strings.ToLower(val))
			default:
				continue LOOP
			}
			switch val {
			case "no":
				comValue = append(comValue, gorocksdb.NoCompression)
			case "snappy":
				comValue = append(comValue, gorocksdb.SnappyCompression)
			case "zlib":
				comValue = append(comValue, gorocksdb.ZLibCompression)
			case "bz2":
				comValue = append(comValue, gorocksdb.Bz2Compression)
			case "lz4":
				comValue = append(comValue, gorocksdb.LZ4Compression)
			case "lz4h":
				comValue = append(comValue, gorocksdb.LZ4HCCompression)
			}
		}
		if len(comValue) > 0 {
			o.SetCompressionPerLevel(comValue)
		}
	}

	// options in the block based table options object
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()

	lcc, ok := config["lru_cache_capacity"].(float64)
	if ok {
		c := gorocksdb.NewLRUCache(int(lcc))
		bbto.SetBlockCache(c)
	}

	bfbpk, ok := config["bloom_filter_bits_per_key"].(float64)
	if ok {
		bf := gorocksdb.NewBloomFilter(int(bfbpk))
		bbto.SetFilterPolicy(bf)
	}

	// set the block based table options
	o.SetBlockBasedTableFactory(bbto)

	return o, nil
}

func (s *Store) newWriteOptions() *gorocksdb.WriteOptions {
	wo := gorocksdb.NewDefaultWriteOptions()

	if s.woptSyncUse {
		wo.SetSync(s.woptSync)
	} else {
		// request fsync on write for safety by default
		wo.SetSync(true)
	}
	if s.woptDisableWALUse {
		wo.DisableWAL(s.woptDisableWAL)
	}

	return wo
}

func (s *Store) newReadOptions() *gorocksdb.ReadOptions {
	ro := gorocksdb.NewDefaultReadOptions()

	if s.roptVerifyChecksumsUse {
		ro.SetVerifyChecksums(s.roptVerifyChecksums)
	}
	if s.roptFillCacheUse {
		ro.SetFillCache(s.roptFillCache)
	}
	if s.roptReadTierUse {
		ro.SetReadTier(gorocksdb.ReadTier(s.roptReadTier))
	}

	return ro
}
