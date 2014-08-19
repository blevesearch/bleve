//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
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
		AddField(document.NewTextField("body", []uint64{}, []byte("A boiling liquid expanding vapor explosion (BLEVE, /ˈblɛviː/ blev-ee) is an explosion caused by the rupture of a vessel containing a pressurized liquid above its boiling point.")))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		indexDocument.ID = strconv.Itoa(i)
		err := index.Update(indexDocument)
		if err != nil {
			b.Fatal(err)
		}
	}

}
