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

package zap

import (
	"regexp/syntax"
)

// Returns the literal prefix given the parse tree for a regexp
func LiteralPrefix(s *syntax.Regexp) string {
	// traverse the left-most branch in the parse tree as long as the
	// node represents a concatenation
	for s != nil && s.Op == syntax.OpConcat {
		if len(s.Sub) < 1 {
			return ""
		}

		s = s.Sub[0]
	}

	if s.Op == syntax.OpLiteral {
		return string(s.Rune)
	}

	return "" // no literal prefix
}
