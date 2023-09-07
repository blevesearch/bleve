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

//go:build gofuzz
// +build gofuzz

package javadata

import "bytes"

func Fuzz(data []byte) int {
	br := bytes.NewReader(data)
	jdr := NewReader(br)

	var err error
	for err == nil {
		_, err = jdr.ReadUTF()
	}
	if err != nil {
		return 0
	}
	return 1
}
