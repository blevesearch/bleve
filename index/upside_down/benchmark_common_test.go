package upside_down

import (
	"strconv"
	"testing"

	"github.com/couchbaselabs/bleve/document"
	"github.com/couchbaselabs/bleve/index/store"
)

func CommonBenchmarkIndex(b *testing.B, s store.KVStore) {

	index := NewUpsideDownCouch(s)

	indexDocument := document.NewDocument("").
		AddField(document.NewTextField("body", []byte("A boiling liquid expanding vapor explosion (BLEVE, /ˈblɛviː/ blev-ee) is an explosion caused by the rupture of a vessel containing a pressurized liquid above its boiling point.")))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		indexDocument.ID = strconv.Itoa(i)
		err := index.Update(indexDocument)
		if err != nil {
			b.Fatal(err)
		}
	}

}
