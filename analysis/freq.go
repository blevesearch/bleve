//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package analysis

import ()

type TokenLocation struct {
	Field    string
	Start    int
	End      int
	Position int
}

type TokenFreq struct {
	Term      []byte
	Locations []*TokenLocation
}

type TokenFrequencies []*TokenFreq

func (tfs TokenFrequencies) MergeAll(remoteField string, other TokenFrequencies) TokenFrequencies {
	// put existing tokens into a map
	index := make(map[string]*TokenFreq)
	for _, tf := range tfs {
		index[string(tf.Term)] = tf
	}
	// walk the new token frequencies
	for _, tf := range other {
		// set the remoteField value in incoming token freqs
		for _, l := range tf.Locations {
			l.Field = remoteField
		}
		existingTf, exists := index[string(tf.Term)]
		if exists {
			existingTf.Locations = append(existingTf.Locations, tf.Locations...)
		} else {
			index[string(tf.Term)] = tf
		}
	}
	// flatten map back to array
	rv := make(TokenFrequencies, len(index))
	i := 0
	for _, tf := range index {
		rv[i] = tf
		i += 1
	}
	return rv
}

func TokenFrequency(tokens TokenStream) TokenFrequencies {
	index := make(map[string]*TokenFreq)

	for _, token := range tokens {
		curr, ok := index[string(token.Term)]
		if ok {
			curr.Locations = append(curr.Locations, &TokenLocation{
				Start:    token.Start,
				End:      token.End,
				Position: token.Position,
			})
		} else {
			index[string(token.Term)] = &TokenFreq{
				Term: token.Term,
				Locations: []*TokenLocation{
					&TokenLocation{
						Start:    token.Start,
						End:      token.End,
						Position: token.Position,
					},
				},
			}
		}
	}

	rv := make(TokenFrequencies, len(index))
	i := 0
	for _, tf := range index {
		rv[i] = tf
		i += 1
	}

	return rv
}
