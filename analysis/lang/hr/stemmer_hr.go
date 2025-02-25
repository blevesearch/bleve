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
	"regexp"
	"strings"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

const StemmerName = "stemmer_hr"

// These regular expressions rules originated from:
// http://nlp.ffzg.hr/resources/tools/stemmer-for-croatian/

var stemmingRules = []*regexp.Regexp{
	regexp.MustCompile(`^(.+(s|š)k)(ijima|ijega|ijemu|ijem|ijim|ijih|ijoj|ijeg|iji|ije|ija|oga|ome|omu|ima|og|om|im|ih|oj|i|e|o|a|u)$`),
	regexp.MustCompile(`^(.+(s|š)tv)(ima|om|o|a|u)$`),
	regexp.MustCompile(`^(.+(t|m|p|r|g)anij)(ama|ima|om|a|u|e|i|)$`),
	regexp.MustCompile(`^(.+an)(inom|ina|inu|ine|ima|in|om|u|i|a|e|)$`),
	regexp.MustCompile(`^(.+in)(ima|ama|om|a|e|i|u|o|)$`),
	regexp.MustCompile(`^(.+on)(ovima|ova|ove|ovi|ima|om|a|e|i|u|)$`),
	regexp.MustCompile(`^(.+n)(ijima|ijega|ijemu|ijeg|ijem|ijim|ijih|ijoj|iji|ije|ija|iju|ima|ome|omu|oga|oj|om|ih|im|og|o|e|a|u|i|)$`),
	regexp.MustCompile(`^(.+(a|e|u)ć)(oga|ome|omu|ega|emu|ima|oj|ih|om|eg|em|og|uh|im|e|a)$`),
	regexp.MustCompile(`^(.+ugov)(ima|i|e|a)$`),
	regexp.MustCompile(`^(.+ug)(ama|om|a|e|i|u|o)$`),
	regexp.MustCompile(`^(.+log)(ama|om|a|u|e|)$`),
	regexp.MustCompile(`^(.+[^eo]g)(ovima|ama|ovi|ove|ova|om|a|e|i|u|o|)$`),
	regexp.MustCompile(`^(.+(rrar|ott|ss|ll)i)(jem|ja|ju|o|)$`),
	regexp.MustCompile(`^(.+uj)(ući|emo|ete|mo|em|eš|e|u|)$`),
	regexp.MustCompile(`^(.+(c|č|ć|đ|l|r)aj)(evima|evi|eva|eve|ama|ima|em|a|e|i|u|)$`),
	regexp.MustCompile(`^(.+(b|c|d|l|n|m|ž|g|f|p|r|s|t|z)ij)(ima|ama|om|a|e|i|u|o|)$`),
	regexp.MustCompile(`^(.+[^z]nal)(ima|ama|om|a|e|i|u|o|)$`),
	regexp.MustCompile(`^(.+ijal)(ima|ama|om|a|e|i|u|o|)$`),
	regexp.MustCompile(`^(.+ozil)(ima|om|a|e|u|i|)$`),
	regexp.MustCompile(`^(.+olov)(ima|i|a|e)$`),
	regexp.MustCompile(`^(.+ol)(ima|om|a|u|e|i|)$`),
	regexp.MustCompile(`^(.+lem)(ama|ima|om|a|e|i|u|o|)$`),
	regexp.MustCompile(`^(.+ram)(ama|om|a|e|i|u|o)$`),
	regexp.MustCompile(`^(.+(a|d|e|o)r)(ama|ima|om|u|a|e|i|)$`),
	regexp.MustCompile(`^(.+(e|i)s)(ima|om|e|a|u)$`),
	regexp.MustCompile(`^(.+(t|n|j|k|j|t|b|g|v)aš)(ama|ima|om|em|a|u|i|e|)$`),
	regexp.MustCompile(`^(.+(e|i)š)(ima|ama|om|em|i|e|a|u|)$`),
	regexp.MustCompile(`^(.+ikat)(ima|om|a|e|i|u|o|)$`),
	regexp.MustCompile(`^(.+lat)(ima|om|a|e|i|u|o|)$`),
	regexp.MustCompile(`^(.+et)(ama|ima|om|a|e|i|u|o|)$`),
	regexp.MustCompile(`^(.+(e|i|k|o)st)(ima|ama|om|a|e|i|u|o|)$`),
	regexp.MustCompile(`^(.+išt)(ima|em|a|e|u)$`),
	regexp.MustCompile(`^(.+ova)(smo|ste|hu|ti|še|li|la|le|lo|t|h|o)$`),
	regexp.MustCompile(`^(.+(a|e|i)v)(ijemu|ijima|ijega|ijeg|ijem|ijim|ijih|ijoj|oga|ome|omu|ima|ama|iji|ije|ija|iju|im|ih|oj|om|og|i|a|u|e|o|)$`),
	regexp.MustCompile(`^(.+[^dkml]ov)(ijemu|ijima|ijega|ijeg|ijem|ijim|ijih|ijoj|oga|ome|omu|ima|iji|ije|ija|iju|im|ih|oj|om|og|i|a|u|e|o|)$`),
	regexp.MustCompile(`^(.+(m|l)ov)(ima|om|a|u|e|i|)$`),
	regexp.MustCompile(`^(.+el)(ijemu|ijima|ijega|ijeg|ijem|ijim|ijih|ijoj|oga|ome|omu|ima|iji|ije|ija|iju|im|ih|oj|om|og|i|a|u|e|o|)$`),
	regexp.MustCompile(`^(.+(a|e|š)nj)(ijemu|ijima|ijega|ijeg|ijem|ijim|ijih|ijoj|oga|ome|omu|ima|iji|ije|ija|iju|ega|emu|eg|em|im|ih|oj|om|og|a|e|i|o|u)$`),
	regexp.MustCompile(`^(.+čin)(ama|ome|omu|oga|ima|og|om|im|ih|oj|a|u|i|o|e|)$`),
	regexp.MustCompile(`^(.+roši)(vši|smo|ste|še|mo|te|ti|li|la|lo|le|m|š|t|h|o)$`),
	regexp.MustCompile(`^(.+oš)(ijemu|ijima|ijega|ijeg|ijem|ijim|ijih|ijoj|oga|ome|omu|ima|iji|ije|ija|iju|im|ih|oj|om|og|i|a|u|e|)$`),
	regexp.MustCompile(`^(.+(e|o)vit)(ijima|ijega|ijemu|ijem|ijim|ijih|ijoj|ijeg|iji|ije|ija|oga|ome|omu|ima|og|om|im|ih|oj|i|e|o|a|u|)$`),
	regexp.MustCompile(`^(.+ast)(ijima|ijega|ijemu|ijem|ijim|ijih|ijoj|ijeg|iji|ije|ija|oga|ome|omu|ima|og|om|im|ih|oj|i|e|o|a|u|)$`),
	regexp.MustCompile(`^(.+k)(ijemu|ijima|ijega|ijeg|ijem|ijim|ijih|ijoj|oga|ome|omu|ima|iji|ije|ija|iju|im|ih|oj|om|og|i|a|u|e|o|)$`),
	regexp.MustCompile(`^(.+(e|a|i|u)va)(jući|smo|ste|jmo|jte|ju|la|le|li|lo|mo|na|ne|ni|no|te|ti|še|hu|h|j|m|n|o|t|v|š|)$`),
	regexp.MustCompile(`^(.+ir)(ujemo|ujete|ujući|ajući|ivat|ujem|uješ|ujmo|ujte|avši|asmo|aste|ati|amo|ate|aju|aše|ahu|ala|alo|ali|ale|uje|uju|uj|al|an|am|aš|at|ah|ao)$`),
	regexp.MustCompile(`^(.+ač)(ismo|iste|iti|imo|ite|iše|eći|ila|ilo|ili|ile|ena|eno|eni|ene|io|im|iš|it|ih|en|i|e)$`),
	regexp.MustCompile(`^(.+ača)(vši|smo|ste|smo|ste|hu|ti|mo|te|še|la|lo|li|le|ju|na|no|ni|ne|o|m|š|t|h|n)$`),
	regexp.MustCompile(`^(.+n)(uvši|usmo|uste|ući|imo|ite|emo|ete|ula|ulo|ule|uli|uto|uti|uta|em|eš|uo|ut|e|u|i)$`),
	regexp.MustCompile(`^(.+ni)(vši|smo|ste|ti|mo|te|mo|te|la|lo|le|li|m|š|o)$`),
	regexp.MustCompile(`^(.+((a|r|i|p|e|u)st|[^o]g|ik|uc|oj|aj|lj|ak|ck|čk|šk|uk|nj|im|ar|at|et|št|it|ot|ut|zn|zv)a)(jući|vši|smo|ste|jmo|jte|jem|mo|te|je|ju|ti|še|hu|la|li|le|lo|na|no|ni|ne|t|h|o|j|n|m|š)$`),
	regexp.MustCompile(`^(.+ur)(ajući|asmo|aste|ajmo|ajte|amo|ate|aju|ati|aše|ahu|ala|ali|ale|alo|ana|ano|ani|ane|al|at|ah|ao|aj|an|am|aš)$`),
	regexp.MustCompile(`^(.+(a|i|o)staj)(asmo|aste|ahu|ati|emo|ete|aše|ali|ući|ala|alo|ale|mo|ao|em|eš|at|ah|te|e|u|)$`),
	regexp.MustCompile(`^(.+(b|c|č|ć|d|e|f|g|j|k|n|r|t|u|v)a)(lama|lima|lom|lu|li|la|le|lo|l)$`),
	regexp.MustCompile(`^(.+(t|č|j|ž|š)aj)(evima|evi|eva|eve|ama|ima|em|a|e|i|u|)$`),
	regexp.MustCompile(`^(.+([^o]m|ič|nč|uč|b|c|ć|d|đ|h|j|k|l|n|p|r|s|š|v|z|ž)a)(jući|vši|smo|ste|jmo|jte|mo|te|ju|ti|še|hu|la|li|le|lo|na|no|ni|ne|t|h|o|j|n|m|š)$`),
	regexp.MustCompile(`^(.+(a|i|o)sta)(dosmo|doste|doše|nemo|demo|nete|dete|nimo|nite|nila|vši|nem|dem|neš|deš|doh|de|ti|ne|nu|du|la|li|lo|le|t|o)$`),
	regexp.MustCompile(`^(.+ta)(smo|ste|jmo|jte|vši|ti|mo|te|ju|še|la|lo|le|li|na|no|ni|ne|n|j|o|m|š|t|h)$`),
	regexp.MustCompile(`^(.+inj)(asmo|aste|ati|emo|ete|ali|ala|alo|ale|aše|ahu|em|eš|at|ah|ao)$`),
	regexp.MustCompile(`^(.+as)(temo|tete|timo|tite|tući|tem|teš|tao|te|li|ti|la|lo|le)$`),
	regexp.MustCompile(`^(.+(elj|ulj|tit|ac|ič|od|oj|et|av|ov)i)(vši|eći|smo|ste|še|mo|te|ti|li|la|lo|le|m|š|t|h|o)$`),
	regexp.MustCompile(`^(.+(tit|jeb|ar|ed|uš|ič)i)(jemo|jete|jem|ješ|smo|ste|jmo|jte|vši|mo|še|te|ti|ju|je|la|lo|li|le|t|m|š|h|j|o)$`),
	regexp.MustCompile(`^(.+(b|č|d|l|m|p|r|s|š|ž)i)(jemo|jete|jem|ješ|smo|ste|jmo|jte|vši|mo|lu|še|te|ti|ju|je|la|lo|li|le|t|m|š|h|j|o)$`),
	regexp.MustCompile(`^(.+luč)(ujete|ujući|ujemo|ujem|uješ|ismo|iste|ujmo|ujte|uje|uju|iše|iti|imo|ite|ila|ilo|ili|ile|ena|eno|eni|ene|uj|io|en|im|iš|it|ih|e|i)$`),
	regexp.MustCompile(`^(.+jeti)(smo|ste|še|mo|te|ti|li|la|lo|le|m|š|t|h|o)$`),
	regexp.MustCompile(`^(.+e)(lama|lima|lom|lu|li|la|le|lo|l)$`),
	regexp.MustCompile(`^(.+i)(lama|lima|lom|lu|li|la|le|lo|l)$`),
	regexp.MustCompile(`^(.+at)(ijega|ijemu|ijima|ijeg|ijem|ijih|ijim|ima|oga|ome|omu|iji|ije|ija|iju|oj|og|om|im|ih|a|u|i|e|o|)$`),
	regexp.MustCompile(`^(.+et)(avši|ući|emo|imo|em|eš|e|u|i)$`),
	regexp.MustCompile(`^(.+)(ajući|alima|alom|avši|asmo|aste|ajmo|ajte|ivši|amo|ate|aju|ati|aše|ahu|ali|ala|ale|alo|ana|ano|ani|ane|am|aš|at|ah|ao|aj|an)$`),
	regexp.MustCompile(`^(.+)(anje|enje|anja|enja|enom|enoj|enog|enim|enih|anom|anoj|anog|anim|anih|eno|ovi|ova|oga|ima|ove|enu|anu|ena|ama)$`),
	regexp.MustCompile(`^(.+)(nijega|nijemu|nijima|nijeg|nijem|nijim|nijih|nima|niji|nije|nija|niju|noj|nom|nog|nim|nih|an|na|nu|ni|ne|no)$`),
	regexp.MustCompile(`^(.+)(om|og|im|ih|em|oj|an|u|o|i|e|a)$`),
}

var highlightVowelRRegex = regexp.MustCompile(`(^|[^aeiou])r($|[^aeiou])`)

func highlightVowelR(term string) string {
	return highlightVowelRRegex.ReplaceAllString(term, `${1}R${2}`)
}

func hasVowel(term string) bool {
	term = highlightVowelR(term)
	return strings.ContainsAny(term, "aeiouR")
}

func stem(term string) string {
	for _, rule := range stemmingRules {
		results := rule.FindStringSubmatch(term)
		if len(results) == 0 {
			continue
		}

		root := results[1]
		if hasVowel(root) && root != "" {
			return root
		}
	}

	return term
}

type CroatianStemmerFilter struct{}

func NewCroatianStemmerFilter() *CroatianStemmerFilter {
	return &CroatianStemmerFilter{}
}

func (s *CroatianStemmerFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	for _, token := range input {
		token.Term = []byte(stem(string(token.Term)))
	}

	return input
}

func CroatianStemmerFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	return NewCroatianStemmerFilter(), nil
}

func init() {
	err := registry.RegisterTokenFilter(StemmerName, CroatianStemmerFilterConstructor)
	if err != nil {
		panic(err)
	}
}
