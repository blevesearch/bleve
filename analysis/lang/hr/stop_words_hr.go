//  Copyright (c) 2020 Couchbase, Inc.
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

package hr

import (
	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

const StopName = "stop_hr"

var CroatianStopWords = []byte(`biti
jesam
budem
sam
jesi
budeš
si
jesmo
budemo
smo
jeste
budete
ste
jesu
budu
su
bih
bijah
bjeh
bijaše
bi
bje
bješe
bijasmo
bismo
bjesmo
bijaste
biste
bjeste
bijahu
biste
bjeste
bijahu
bi
biše
bjehu
bješe
bio
bili
budimo
budite
bila
bilo
bile
ću
ćeš
će
ćemo
ćete
želim
želiš
želi
želimo
želite
žele
moram
moraš
mora
moramo
morate
moraju
trebam
trebaš
treba
trebamo
trebate
trebaju
mogu
možeš
može
možemo
možete
za
`)

func TokenMapConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenMap, error) {
	rv := analysis.NewTokenMap()
	err := rv.LoadBytes(CroatianStopWords)

	return rv, err
}

func init() {
	err := registry.RegisterTokenMap(StopName, TokenMapConstructor)
	if err != nil {
		panic(err)
	}
}
