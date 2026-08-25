package lt

import (
	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

const StopName = "stop_lt"

// this content was obtained from:
// https://github.com/apache/lucene-solr/blob/master/lucene/analysis/common/src/resources/org/apache/lucene/analysis/lt/stopwords.txt

var LithuanianStopWords = []byte(`# Lithuanian stopwords list
ant
apie
ar
arba
aš
be
bei
bet
bus
būti
būtų
buvo
dėl
gali
į
iki
ir
iš
ja
ją
jai
jais
jam
jame
jas
jei
ji
jį
jie
jiedu
jiedvi
jiedviem
jiedviese
jiems
jis
jo
jodviem
jog
joje
jomis
joms
jos
jose
jų
judu
judvi
judviejų
jųdviejų
judviem
judviese
jumis
jums
jumyse
juo
juodu
juodviese
juos
juose
jus
jūs
jūsų
ką
kad
kai
kaip
kas
kiek
kol
kur
kurie
kuris
man
mane
manęs
manimi
mano
manyje
mes
metu
mudu
mudvi
mudviejų
mudviem
mudviese
mumis
mums
mumyse
mus
mūsų
nei
nes
net
nors
nuo
o
pat
per
po
prie
prieš
sau
save
savęs
savimi
savo
savyje
su
tačiau
tada
tai
taip
tas
tau
tave
tavęs
tavimi
tavyje
tiek
ten
to
todėl
tu
tuo
už
visi
yra
`)

func TokenMapConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenMap, error) {
	rv := analysis.NewTokenMap()
	err := rv.LoadBytes(LithuanianStopWords)
	return rv, err
}

func init() {
	registry.RegisterTokenMap(StopName, TokenMapConstructor)
}
