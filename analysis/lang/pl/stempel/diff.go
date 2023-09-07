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

// Diff transforms the dest rune slice following the rules described
// in the diff command rune slice.
func Diff(dest, diff []rune) []rune {
	if len(diff) == 0 {
		return dest
	}

	pos := len(dest) - 1
	if pos < 0 {
		return dest
	}

	for i := 0; i < len(diff)/2; i++ {
		cmd := diff[2*i]
		param := diff[2*i+1]
		parNum := int(param - 'a' + 1)
		switch cmd {
		case '-':
			pos = pos - parNum + 1
		case 'R':
			if pos < 0 || pos >= len(dest) {
				// out of bounds, just return
				return dest
			}
			dest[pos] = param
		case 'D':
			o := pos
			pos -= parNum - 1
			if pos < 0 || pos >= len(dest) {
				// out of bounds, just return
				return dest
			}
			dest = append(dest[:pos], dest[o+1:]...)
		case 'I':
			pos++
			if pos < 0 || pos > len(dest) {
				// out of bounds, just return
				return dest
			}

			dest = append(dest, 0)
			copy(dest[pos+1:], dest[pos:])
			dest[pos] = param
		}
		pos--
	}
	return dest
}
