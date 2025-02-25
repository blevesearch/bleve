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
	"strings"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

const SuffixTransformationFilterName = "hr_suffix_transformation_filter"

var SuffixTransformations = map[string]string{
	"lozi":     "loga",
	"lozima":   "loga",
	"pjesi":    "pjeh",
	"pjesima":  "pjeh",
	"vojci":    "vojka",
	"bojci":    "bojka",
	"jaci":     "jak",
	"jacima":   "jak",
	"čajan":    "čajni",
	"ijeran":   "ijerni",
	"laran":    "larni",
	"ijesan":   "ijesni",
	"anjac":    "anjca",
	"ajac":     "ajca",
	"ajaca":    "ajca",
	"ljaca":    "ljca",
	"ljac":     "ljca",
	"ejac":     "ejca",
	"ejaca":    "ejca",
	"ojac":     "ojca",
	"ojaca":    "ojca",
	"ajaka":    "ajka",
	"ojaka":    "ojka",
	"šaca":     "šca",
	"šac":      "šca",
	"inzima":   "ing",
	"inzi":     "ing",
	"tvenici":  "tvenik",
	"tetici":   "tetika",
	"teticima": "tetika",
	"nstava":   "nstva",
	"nicima":   "nik",
	"ticima":   "tik",
	"zicima":   "zik",
	"snici":    "snik",
	"kuse":     "kusi",
	"kusan":    "kusni",
	"kustava":  "kustva",
	"dušan":    "dušni",
	"antan":    "antni",
	"bilan":    "bilni",
	"tilan":    "tilni",
	"avilan":   "avilni",
	"silan":    "silni",
	"gilan":    "gilni",
	"rilan":    "rilni",
	"nilan":    "nilni",
	"alan":     "alni",
	"ozan":     "ozni",
	"rave":     "ravi",
	"stavan":   "stavni",
	"pravan":   "pravni",
	"tivan":    "tivni",
	"sivan":    "sivni",
	"atan":     "atni",
	"cenata":   "centa",
	"denata":   "denta",
	"genata":   "genta",
	"lenata":   "lenta",
	"menata":   "menta",
	"jenata":   "jenta",
	"venata":   "venta",
	"tetan":    "tetni",
	"pletan":   "pletni",
	"šave":     "šavi",
	"manata":   "manta",
	"tanata":   "tanta",
	"lanata":   "lanta",
	"sanata":   "santa",
	"ačak":     "ačka",
	"ačaka":    "ačka",
	"ušak":     "uška",
	"atak":     "atka",
	"ataka":    "atka",
	"atci":     "atka",
	"atcima":   "atka",
	"etak":     "etka",
	"etaka":    "etka",
	"itak":     "itka",
	"itaka":    "itka",
	"itci":     "itka",
	"otak":     "otka",
	"otaka":    "otka",
	"utak":     "utka",
	"utaka":    "utka",
	"utci":     "utka",
	"utcima":   "utka",
	"eskan":    "eskna",
	"tičan":    "tični",
	"ojsci":    "ojska",
	"esama":    "esma",
	"metara":   "metra",
	"centar":   "centra",
	"centara":  "centra",
	"istara":   "istra",
	"istar":    "istra",
	"ošću":     "osti",
	"daba":     "dba",
	"čcima":    "čka",
	"čci":      "čka",
	"mac":      "mca",
	"maca":     "mca",
	"voljan":   "voljni",
	"anaka":    "anki",
	"vac":      "vca",
	"vaca":     "vca",
	"saca":     "sca",
	"sac":      "sca",
	"naca":     "nca",
	"nac":      "nca",
	"raca":     "rca",
	"rac":      "rca",
	"aoca":     "alca",
	"alaca":    "alca",
	"alac":     "alca",
	"elaca":    "elca",
	"elac":     "elca",
	"olaca":    "olca",
	"olac":     "olca",
	"olce":     "olca",
	"njac":     "njca",
	"njaca":    "njca",
	"ekata":    "ekta",
	"ekat":     "ekta",
	"izam":     "izma",
	"izama":    "izma",
	"jebe":     "jebi",
	"ašan":     "ašni",
}

type SuffixTransformationFilter struct{}

func NewSuffixTransformationFilter() *SuffixTransformationFilter {
	return &SuffixTransformationFilter{}
}

func (s *SuffixTransformationFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	for _, token := range input {
		term := string(token.Term)

		for suffix, newSuffix := range SuffixTransformations {
			if strings.HasSuffix(term, suffix) {
				term = term[:len(term)-len(suffix)] + newSuffix
				break
			}
		}

		token.Term = []byte(term)
	}

	return input
}

func SuffixTransformationFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	return NewSuffixTransformationFilter(), nil
}

func init() {
	err := registry.RegisterTokenFilter(SuffixTransformationFilterName, SuffixTransformationFilterConstructor)
	if err != nil {
		panic(err)
	}
}
