package leveldb

import (
	"github.com/jmhodges/levigo"
)

func defaultWriteOptions() *levigo.WriteOptions {
	wo := levigo.NewWriteOptions()
	// request fsync on write for safety
	wo.SetSync(true)
	return wo
}

func defaultReadOptions() *levigo.ReadOptions {
	ro := levigo.NewReadOptions()
	return ro
}
