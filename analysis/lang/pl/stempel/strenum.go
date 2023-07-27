//  Copyright (c) 2018 Couchbase, Inc.
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

package stempel

import (
	"io"
)

type strEnum struct {
	r    []rune
	from int
	by   int
}

func newStrEnum(s []rune, up bool) *strEnum {
	rv := &strEnum{
		r: s,
	}
	if up {
		rv.from = 0
		rv.by = 1
	} else {
		rv.from = len(s) - 1
		rv.by = -1
	}
	return rv
}

func (s *strEnum) next() (rune, error) {
	if s.from < 0 || s.from >= len(s.r) {
		return 0, io.EOF
	}
	rv := s.r[s.from]
	s.from += s.by
	return rv, nil
}
