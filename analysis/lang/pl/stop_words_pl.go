package pl

import (
	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

const StopName = "stop_pl"

// This content was obtained from:
// https://github.com/stopwords-iso/stopwords-pl/blob/master/stopwords-pl.txt

// Removed non-unicode coding.
// All credits go to:
// https://raw.githubusercontent.com/stopwords-iso/stopwords-iso/master/CREDITS.md
// Following license applies
// The MIT License (MIT)

// Copyright (c) 2016 Gene Diaz

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

var PolishStopWords = []byte(`a
aby
ach
acz
aczkolwiek
aj
albo
ale
ależ
ani
aż
bardziej
bardzo
bez
bo
bowiem
by
byli
bym
bynajmniej
być
był
była
było
były
będzie
będą
cali
cała
cały
chce
choć
ci
ciebie
cię
co
cokolwiek
coraz
coś
czasami
czasem
czemu
czy
czyli
często
daleko
dla
dlaczego
dlatego
do
dobrze
dokąd
dość
dr
dużo
dwa
dwaj
dwie
dwoje
dzisiaj
dziś
gdy
gdyby
gdyż
gdzie
gdziekolwiek
gdzieś
go
godz
hab
i
ich
ii
iii
ile
im
inna
inne
inny
innych
inż
iv
ix
iż
ja
jak
jakaś
jakby
jaki
jakichś
jakie
jakiś
jakiż
jakkolwiek
jako
jakoś
je
jeden
jedna
jednak
jednakże
jedno
jednym
jedynie
jego
jej
jemu
jest
jestem
jeszcze
jeśli
jeżeli
już
ją
każdy
kiedy
kierunku
kilka
kilku
kimś
kto
ktokolwiek
ktoś
która
które
którego
której
który
których
którym
którzy
ku
lat
lecz
lub
ma
mają
mam
mamy
mało
mgr
mi
miał
mimo
między
mnie
mną
mogą
moi
moim
moja
moje
może
możliwe
można
mu
musi
my
mój
na
nad
nam
nami
nas
nasi
nasz
nasza
nasze
naszego
naszych
natomiast
natychmiast
nawet
nic
nich
nie
niech
niego
niej
niemu
nigdy
nim
nimi
nią
niż
no
nowe
np
nr
o
obok
od
ok
około
on
ona
one
oni
ono
oraz
oto
owszem
pan
pana
pani
pl
po
pod
podczas
pomimo
ponad
ponieważ
powinien
powinna
powinni
powinno
poza
prawie
prof
przecież
przed
przede
przedtem
przez
przy
raz
razie
roku
również
sam
sama
się
skąd
sobie
sobą
sposób
swoje
są
ta
tak
taka
taki
takich
takie
także
tam
te
tego
tej
tel
temu
ten
teraz
też
to
tobie
tobą
toteż
totobą
trzeba
tu
tutaj
twoi
twoim
twoja
twoje
twym
twój
ty
tych
tylko
tym
tys
tzw
tę
u
ul
vi
vii
viii
vol
w
wam
wami
was
wasi
wasz
wasza
wasze
we
według
wie
wiele
wielu
więc
więcej
wszyscy
wszystkich
wszystkie
wszystkim
wszystko
wtedy
www
wy
właśnie
wśród
xi
xii
xiii
xiv
xv
z
za
zapewne
zawsze
zaś
ze
zeznowu
znowu
znów
został
zł
żaden
żadna
żadne
żadnych
że
żeby
`)

func TokenMapConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenMap, error) {
	rv := analysis.NewTokenMap()
	err := rv.LoadBytes(PolishStopWords)
	return rv, err
}

func init() {
	registry.RegisterTokenMap(StopName, TokenMapConstructor)
}
