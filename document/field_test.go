//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package document

import (
	"testing"
	"time"
)

func TestCompareFieldValues(t *testing.T) {

	t1 := time.Now()
	t2 := t1.Add(1 * time.Hour)

	dtf1, _ := NewDateTimeField("", nil, t1)
	dtf2, _ := NewDateTimeField("", nil, t2)

	tests := []struct {
		l    Field
		r    Field
		desc bool
		res  int
	}{
		// nil simple
		{
			l:   nil,
			r:   nil,
			res: 0,
		},
		// boolean simple
		{
			l:   NewBooleanField("", nil, true),
			r:   NewBooleanField("", nil, true),
			res: 0,
		},
		{
			l:   NewBooleanField("", nil, true),
			r:   NewBooleanField("", nil, false),
			res: 1,
		},
		{
			l:   NewBooleanField("", nil, false),
			r:   NewBooleanField("", nil, true),
			res: -1,
		},
		{
			l:    NewBooleanField("", nil, true),
			r:    NewBooleanField("", nil, false),
			desc: true,
			res:  -1,
		},
		{
			l:    NewBooleanField("", nil, false),
			r:    NewBooleanField("", nil, true),
			desc: true,
			res:  1,
		},
		// numeric simple
		{
			l:   NewNumericField("", nil, 3.14),
			r:   NewNumericField("", nil, 3.14),
			res: 0,
		},
		{
			l:   NewNumericField("", nil, 5.14),
			r:   NewNumericField("", nil, 3.14),
			res: 1,
		},
		{
			l:   NewNumericField("", nil, 3.14),
			r:   NewNumericField("", nil, 5.14),
			res: -1,
		},
		{
			l:    NewNumericField("", nil, 5.14),
			r:    NewNumericField("", nil, 3.14),
			desc: true,
			res:  -1,
		},
		{
			l:    NewNumericField("", nil, 3.14),
			r:    NewNumericField("", nil, 5.14),
			desc: true,
			res:  1,
		},
		// text simple
		{
			l:   NewTextField("", nil, []byte("cat")),
			r:   NewTextField("", nil, []byte("cat")),
			res: 0,
		},
		{
			l:   NewTextField("", nil, []byte("dog")),
			r:   NewTextField("", nil, []byte("cat")),
			res: 1,
		},
		{
			l:   NewTextField("", nil, []byte("cat")),
			r:   NewTextField("", nil, []byte("dog")),
			res: -1,
		},
		{
			l:    NewTextField("", nil, []byte("dog")),
			r:    NewTextField("", nil, []byte("cat")),
			desc: true,
			res:  -1,
		},
		{
			l:    NewTextField("", nil, []byte("cat")),
			r:    NewTextField("", nil, []byte("dog")),
			desc: true,
			res:  1,
		},
		// datetime simple
		{
			l:   dtf1,
			r:   dtf1,
			res: 0,
		},
		{
			l:   dtf2,
			r:   dtf1,
			res: 1,
		},
		{
			l:   dtf1,
			r:   dtf2,
			res: -1,
		},
		{
			l:    dtf2,
			r:    dtf1,
			desc: true,
			res:  -1,
		},
		{
			l:    dtf1,
			r:    dtf2,
			desc: true,
			res:  1,
		},
		// mixed types, nil left
		{
			l:   nil,
			r:   NewBooleanField("", nil, true),
			res: -1,
		},
		{
			l:    nil,
			r:    NewBooleanField("", nil, true),
			desc: true,
			res:  1,
		},
		{
			l:   nil,
			r:   NewNumericField("", nil, 3.14),
			res: -1,
		},
		{
			l:    nil,
			r:    NewNumericField("", nil, 3.14),
			desc: true,
			res:  1,
		},
		{
			l:   nil,
			r:   NewTextField("", nil, []byte("cat")),
			res: -1,
		},
		{
			l:    nil,
			r:    NewTextField("", nil, []byte("cat")),
			desc: true,
			res:  1,
		},
		{
			l:   nil,
			r:   dtf1,
			res: -1,
		},
		{
			l:    nil,
			r:    dtf1,
			desc: true,
			res:  1,
		},
		// mixed types, boolean left
		{
			l:   NewBooleanField("", nil, true),
			r:   nil,
			res: 1,
		},
		{
			l:    NewBooleanField("", nil, true),
			r:    nil,
			desc: true,
			res:  -1,
		},
		{
			l:   NewBooleanField("", nil, true),
			r:   NewNumericField("", nil, 3.14),
			res: -1,
		},
		{
			l:    NewBooleanField("", nil, true),
			r:    NewNumericField("", nil, 3.14),
			desc: true,
			res:  1,
		},
		{
			l:   NewBooleanField("", nil, true),
			r:   NewTextField("", nil, []byte("cat")),
			res: -1,
		},
		{
			l:    NewBooleanField("", nil, true),
			r:    NewTextField("", nil, []byte("cat")),
			desc: true,
			res:  1,
		},
		{
			l:   NewBooleanField("", nil, true),
			r:   dtf1,
			res: -1,
		},
		{
			l:    NewBooleanField("", nil, true),
			r:    dtf1,
			desc: true,
			res:  1,
		},
		// mixed types, number left
		{
			l:   NewNumericField("", nil, 3.14),
			r:   nil,
			res: 1,
		},
		{
			l:    NewNumericField("", nil, 3.14),
			r:    nil,
			desc: true,
			res:  -1,
		},
		{
			l:   NewNumericField("", nil, 3.14),
			r:   NewBooleanField("", nil, true),
			res: 1,
		},
		{
			l:    NewNumericField("", nil, 3.14),
			r:    NewBooleanField("", nil, true),
			desc: true,
			res:  -1,
		},
		{
			l:   NewNumericField("", nil, 3.14),
			r:   NewTextField("", nil, []byte("cat")),
			res: -1,
		},
		{
			l:    NewNumericField("", nil, 3.14),
			r:    NewTextField("", nil, []byte("cat")),
			desc: true,
			res:  1,
		},
		{
			l:   NewNumericField("", nil, 3.14),
			r:   dtf1,
			res: -1,
		},
		{
			l:    NewNumericField("", nil, 3.14),
			r:    dtf1,
			desc: true,
			res:  1,
		},
		// mixed types, text left
		{
			l:   NewTextField("", nil, []byte("cat")),
			r:   nil,
			res: 1,
		},
		{
			l:    NewTextField("", nil, []byte("cat")),
			r:    nil,
			desc: true,
			res:  -1,
		},
		{
			l:   NewTextField("", nil, []byte("cat")),
			r:   NewBooleanField("", nil, true),
			res: 1,
		},
		{
			l:    NewTextField("", nil, []byte("cat")),
			r:    NewBooleanField("", nil, true),
			desc: true,
			res:  -1,
		},
		{
			l:   NewTextField("", nil, []byte("cat")),
			r:   NewNumericField("", nil, 3.14),
			res: 1,
		},
		{
			l:    NewTextField("", nil, []byte("cat")),
			r:    NewNumericField("", nil, 3.14),
			desc: true,
			res:  -1,
		},
		{
			l:   NewTextField("", nil, []byte("cat")),
			r:   dtf1,
			res: -1,
		},
		{
			l:    NewTextField("", nil, []byte("cat")),
			r:    dtf1,
			desc: true,
			res:  1,
		},
		// mixed types, datetimes left
		{
			l:   dtf1,
			r:   nil,
			res: 1,
		},
		{
			l:    dtf1,
			r:    nil,
			desc: true,
			res:  -1,
		},
		{
			l:   dtf1,
			r:   NewBooleanField("", nil, true),
			res: 1,
		},
		{
			l:    dtf1,
			r:    NewBooleanField("", nil, true),
			desc: true,
			res:  -1,
		},
		{
			l:   dtf1,
			r:   NewNumericField("", nil, 3.14),
			res: 1,
		},
		{
			l:    dtf1,
			r:    NewNumericField("", nil, 3.14),
			desc: true,
			res:  -1,
		},
		{
			l:   dtf1,
			r:   NewTextField("", nil, []byte("cat")),
			res: 1,
		},
		{
			l:    dtf1,
			r:    NewTextField("", nil, []byte("cat")),
			desc: true,
			res:  -1,
		},
	}

	for i, test := range tests {
		actual := CompareFieldValues(test.l, test.r, test.desc)
		if actual != test.res {
			t.Errorf("expected %d, got %d for case %d", test.res, actual, i)
		}
	}

}
