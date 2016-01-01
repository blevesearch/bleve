//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package analysis

// TokenLocation represents one occurrence of a term at a particular location in
// a field. Start, End and Position have the same meaning as in analysis.Token.
// Field and ArrayPositions identify the field value in the source document.
// See document.Field for details.
type TokenLocation struct {
	Field          string
	ArrayPositions []uint64
	Start          int
	End            int
	Position       int
}

// TokenFreq represents all the occurrences of a term in all fields of a
// document.
type TokenFreq struct {
	Term      []byte
	Locations []*TokenLocation
}

func (tf *TokenFreq) Frequency() int {
	return len(tf.Locations)
}

// TokenFrequencies maps document terms to their combined frequencies from all
// fields.
type TokenFrequencies map[string]*TokenFreq

func (tfs TokenFrequencies) MergeAll(remoteField string, other TokenFrequencies) {
	// walk the new token frequencies
	for tfk, tf := range other {
		// set the remoteField value in incoming token freqs
		for _, l := range tf.Locations {
			l.Field = remoteField
		}
		existingTf, exists := tfs[tfk]
		if exists {
			existingTf.Locations = append(existingTf.Locations, tf.Locations...)
		} else {
			tfs[tfk] = tf
		}
	}
}

func TokenFrequency(tokens TokenStream, arrayPositions []uint64) TokenFrequencies {
	rv := make(map[string]*TokenFreq, len(tokens))

	for _, token := range tokens {
		term := string(token.Term)
		curr, ok := rv[term]
		if ok {
			curr.Locations = append(curr.Locations, &TokenLocation{
				ArrayPositions: arrayPositions,
				Start:          token.Start,
				End:            token.End,
				Position:       token.Position,
			})
		} else {
			rv[term] = &TokenFreq{
				Term: token.Term,
				Locations: []*TokenLocation{
					&TokenLocation{
						ArrayPositions: arrayPositions,
						Start:          token.Start,
						End:            token.End,
						Position:       token.Position,
					},
				},
			}
		}
	}

	return rv
}
