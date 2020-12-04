package cro

import (
	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

const StopName = "stop_cro"

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
	registry.RegisterTokenMap(StopName, TokenMapConstructor)
}
