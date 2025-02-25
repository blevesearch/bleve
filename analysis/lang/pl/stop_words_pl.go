package pl

import (
	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

const StopName = "stop_pl"

// this content was obtained from:
// lucene-4.7.2/analysis/common/src/resources/org/apache/lucene/analysis/snowball/
// ` was changed to ' to allow for literal string

var PolishStopWords = []byte(` | From https://github.com/stopwords-iso/stopwords-pl/tree/master
 | The MIT License (MIT)
 | See https://github.com/stopwords-iso/stopwords-pl/blob/master/LICENSE
 |  - Encoding was converted to UTF-8.
 |  - This notice was added.
 |  - english text is auto-translate
 |
 | NOTE: To use this file with StopFilterFactory, you must specify format="snowball"
 | a polish stop word list. comments begin with vertical bar. each stop
 | word is at the start of a line.

a				| and
aby				| to
ach				| ah
acz				| although
aczkolwiek		| although
aj				| ay
albo			| or
ale				| but
ależ			| but
ani				| or
aż				| until
bardziej		| more
bardzo			| very
bez				| without
bo				| because
bowiem			| because
by				| by
byli			| were
bym				| i would
bynajmniej		| not at all
być				| to be
był				| was
była			| was
było			| was
były			| were
będzie			| will be
będą			| they will
cali			| inches
cała			| whole
cały			| whole
chce			| i want
choć			| though
ci				| you
ciebie			| you
cię				| you
co				| what
cokolwiek		| whatever
coraz			| getting
coś				| something
czasami			| sometimes
czasem			| sometimes
czemu			| why
czy				| whether
czyli			| that is
często			| often
daleko			| far
dla				| for
dlaczego		| why
dlatego			| which is why
do				| down
dobrze			| all right
dokąd			| where
dość			| enough
dr				| dr
dużo			| a lot
dwa				| two
dwaj			| two
dwie			| two
dwoje			| two
dzisiaj			| today
dziś			| today
gdy				| when
gdyby			| if
gdyż			| because
gdzie			| where
gdziekolwiek	| wherever
gdzieś			| somewhere
go				| him
godz			| time
hab				| hab
i				| and
ich				| their
ii				| ii
iii				| iii
ile				| how much
im				| them
inna			| different
inne			| other
inny			| other
innych			| other
inż				| eng
iv				| iv
ix				| ix
iż				| that
ja				| i
jak				| how
jakaś			| some
jakby			| as if
jaki			| what
jakichś			| some
jakie			| what
jakiś			| some
jakiż			| what
jakkolwiek		| however
jako			| as
jakoś			| somehow
je				| them
jeden			| one
jedna			| one
jednak			| but
jednakże		| however
jedno			| one
jednym			| one
jedynie			| only
jego			| his
jej				| her
jemu			| him
jest			| is
jestem			| i am
jeszcze			| still
jeśli			| if
jeżeli			| if
już				| already
ją				| i
każdy			| everyone
kiedy			| when
kierunku		| direction
kilka			| several
kilku			| several
kimś			| someone
kto				| who
ktokolwiek		| anyone
ktoś			| someone
która			| which
które			| which
którego			| whose
której			| which
który			| which
których			| which
którym			| which
którzy			| who
ku				| to
lat				| years
lecz			| but
lub				| or
ma				| has
mają			| may
mam				| i have
mamy			| we have
mało			| little
mgr				| msc
mi				| to me
miał			| had
mimo			| despite
między			| between
mnie			| me
mną				| me
mogą			| they can
moi				| my
moim			| my
moja			| my
moje			| my
może			| maybe
możliwe			| that's possible
można			| you can
mu				| him
musi			| has to
my				| we
mój				| my
na				| on
nad				| above
nam				| u.s
nami			| us
nas				| us
nasi			| our
nasz			| our
nasza			| our
nasze			| our
naszego			| our
naszych			| ours
natomiast		| whereas
natychmiast		| immediately
nawet			| even
nic				| nothing
nich			| them
nie				| no
niech			| let
niego			| him
niej			| her
niemu			| not him
nigdy			| never
nim				| him
nimi			| them
nią				| her
niż				| than
no				| yeah
nowe			| new
np				| e.g.
nr				| no
o				| about
o.o.			| o.o.
obok			| near
od				| from
ok				| approx
około			| about
on				| he
ona				| she
one				| they
oni				| they
ono				| it
oraz			| and
oto				| here
owszem			| yes
pan				| mr
pana			| mr
pani			| you
pl				| pl
po				| after
pod				| under
podczas			| while
pomimo			| despite
ponad			| above
ponieważ		| because
powinien		| should
powinna			| she should
powinni			| they should
powinno			| should
poza			| apart from
prawie			| almost
prof			| prof
przecież		| yet
przed			| before
przede			| above
przedtem		| before
przez			| by
przy			| by
raz				| once
razie			| case
roku			| year
również			| also
sam				| alone
sama			| alone
się				| myself
skąd			| from where
sobie			| myself
sobą			| myself
sposób			| way
swoje			| own
są				| are
ta				| this
tak				| yes
taka			| such
taki			| such
takich			| such
takie			| such
także			| too
tam				| over there
te				| these
tego			| this
tej				| this one
tel				| phone
temu			| ago
ten				| this
teraz			| now
też				| too
to				| this
tobie			| you
tobą			| you
toteż			| this as well
totobą			| you
trzeba			| it's necessary to
tu				| here
tutaj			| here
twoi			| yours
twoim			| yours
twoja			| your
twoje			| your
twym			| your
twój			| your
ty				| you
tych			| these
tylko			| just
tym				| this
tys				| thousand
tzw				| so-called
tę				| these
u				| at
ul				| st
vi				| vi
vii				| vii
viii			| viii
vol				| vol
w				| in
wam				| you
wami			| you
was				| mustache
wasi			| yours
wasz			| yours
wasza			| yours
wasze			| yours
we				| in
według			| according to
wie				| knows
wiele			| many
wielu			| many
więc			| so
więcej			| more
wszyscy			| all
wszystkich		| everyone
wszystkie		| all
wszystkim		| everyone
wszystko		| all
wtedy			| then
www				| www
wy				| you
właśnie			| exactly
wśród			| among
xi				| x.x
xii				| xii
xiii			| xii
xiv				| xiv
xv				| xv
z				| with
za				| behind
zapewne			| probably
zawsze			| always
zaś				| and
ze				| that
zeznowu			| testify
znowu			| again
znów			| again
został			| left
zł				| zloty
żaden			| no
żadna			| none
żadne			| none
żadnych			| none
że				| that
żeby			| to

`)

func TokenMapConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenMap, error) {
	rv := analysis.NewTokenMap()
	err := rv.LoadBytes(PolishStopWords)
	return rv, err
}

func init() {
	err := registry.RegisterTokenMap(StopName, TokenMapConstructor)
	if err != nil {
		panic(err)
	}
}
