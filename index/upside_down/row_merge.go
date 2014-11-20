//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package upside_down

type termSummaryIncr struct{}

func newTermSummaryIncr() *termSummaryIncr {
	return &termSummaryIncr{}
}

func (t *termSummaryIncr) Merge(key, existing []byte) ([]byte, error) {
	if len(existing) > 0 {
		tfr, err := NewTermFrequencyRowKV(key, existing)
		if err != nil {
			return nil, err
		}
		tfr.freq++
		return tfr.Value(), nil
	} else {
		tfr, err := NewTermFrequencyRowK(key)
		if err != nil {
			return nil, err
		}
		tfr.freq = 1
		return tfr.Value(), nil
	}
}

type termSummaryDecr struct{}

func newTermSummaryDecr() *termSummaryDecr {
	return &termSummaryDecr{}
}

func (t *termSummaryDecr) Merge(key, existing []byte) ([]byte, error) {
	if len(existing) > 0 {
		tfr, err := NewTermFrequencyRowKV(key, existing)
		if err != nil {
			return nil, err
		}
		tfr.freq--
		if tfr.freq > 0 {
			return tfr.Value(), nil
		}
	}
	return nil, nil
}
