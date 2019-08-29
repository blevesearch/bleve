//  Copyright (c) 2019 Couchbase, Inc.
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

package reverse

import (
	"bytes"
	"testing"

	"github.com/blugelabs/bleve/analysis"
)

func TestReverseFilter(t *testing.T) {
	inputTokenStream := analysis.TokenStream{
		&analysis.Token{},
		&analysis.Token{
			Term: []byte("one"),
		},
		&analysis.Token{
			Term: []byte("TWo"),
		},
		&analysis.Token{
			Term: []byte("thRee"),
		},
		&analysis.Token{
			Term: []byte("four's"),
		},
		&analysis.Token{
			Term: []byte("what's this in reverse"),
		},
		&analysis.Token{
			Term: []byte("œ∑´®†"),
		},
		&analysis.Token{
			Term: []byte("İȺȾCAT÷≥≤µ123"),
		},
		&analysis.Token{
			Term: []byte("!@#$%^&*()"),
		},
		&analysis.Token{
			Term: []byte("cafés"),
		},
		&analysis.Token{
			Term: []byte("¿Dónde estás?"),
		},
		&analysis.Token{
			Term: []byte("Me gustaría una cerveza."),
		},
	}

	expectedTokenStream := analysis.TokenStream{
		&analysis.Token{},
		&analysis.Token{
			Term: []byte("eno"),
		},
		&analysis.Token{
			Term: []byte("oWT"),
		},
		&analysis.Token{
			Term: []byte("eeRht"),
		},
		&analysis.Token{
			Term: []byte("s'ruof"),
		},
		&analysis.Token{
			Term: []byte("esrever ni siht s'tahw"),
		},
		&analysis.Token{
			Term: []byte("†®´∑œ"),
		},
		&analysis.Token{
			Term: []byte("321µ≤≥÷TACȾȺİ"),
		},
		&analysis.Token{
			Term: []byte(")(*&^%$#@!"),
		},
		&analysis.Token{
			Term: []byte("séfac"),
		},
		&analysis.Token{
			Term: []byte("?sátse ednóD¿"),
		},
		&analysis.Token{
			Term: []byte(".azevrec anu aíratsug eM"),
		},
	}

	filter := NewReverseFilter()
	outputTokenStream := filter.Filter(inputTokenStream)
	for i := 0; i < len(expectedTokenStream); i++ {
		if !bytes.Equal(outputTokenStream[i].Term, expectedTokenStream[i].Term) {
			t.Errorf("[%d] expected %s got %s",
				i+1, expectedTokenStream[i].Term, outputTokenStream[i].Term)
		}
	}
}

func BenchmarkReverseFilter(b *testing.B) {
	input := analysis.TokenStream{
		&analysis.Token{
			Term: []byte("A"),
		},
		&analysis.Token{
			Term: []byte("boiling"),
		},
		&analysis.Token{
			Term: []byte("liquid"),
		},
		&analysis.Token{
			Term: []byte("expanding"),
		},
		&analysis.Token{
			Term: []byte("vapor"),
		},
		&analysis.Token{
			Term: []byte("explosion"),
		},
		&analysis.Token{
			Term: []byte("caused"),
		},
		&analysis.Token{
			Term: []byte("by"),
		},
		&analysis.Token{
			Term: []byte("the"),
		},
		&analysis.Token{
			Term: []byte("rupture"),
		},
		&analysis.Token{
			Term: []byte("of"),
		},
		&analysis.Token{
			Term: []byte("a"),
		},
		&analysis.Token{
			Term: []byte("vessel"),
		},
		&analysis.Token{
			Term: []byte("containing"),
		},
		&analysis.Token{
			Term: []byte("pressurized"),
		},
		&analysis.Token{
			Term: []byte("liquid"),
		},
		&analysis.Token{
			Term: []byte("above"),
		},
		&analysis.Token{
			Term: []byte("its"),
		},
		&analysis.Token{
			Term: []byte("boiling"),
		},
		&analysis.Token{
			Term: []byte("point"),
		},
		&analysis.Token{
			Term: []byte("İȺȾCAT"),
		},
		&analysis.Token{
			Term: []byte("Me gustaría una cerveza."),
		},
	}
	filter := NewReverseFilter()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Filter(input)
	}
}
