//  Copyright (c) 2017 Couchbase, Inc.
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

package zap

import (
	"io"
	"io/ioutil"
)

func (s *SegmentBase) readMem(x, y uint64) []byte {
	if s.mem == nil {
		data, _ := ioutil.ReadAll(io.NewSectionReader(s.f, int64(x), int64(y-x)))
		return data
	}
	return s.mem[x:y]
}

func (s *Segment) readMM(x, y int) []byte {
	if s.mm == nil {
		data, _ := ioutil.ReadAll(io.NewSectionReader(s.f, int64(x), int64(y-x)))
		return data
	}
	return s.mm[x:y]
}
