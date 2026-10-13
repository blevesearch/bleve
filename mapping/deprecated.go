//  Copyright (c) 2026 Couchbase, Inc.
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

package mapping

import (
	"errors"

	"github.com/blevesearch/bleve/v2/registry"
)

// skipDeprecated lets validation carry on past a deprecated analysis
// component.  It returns nil for an error wrapping
// registry.ErrDeprecatedComponent, keeping the first such error in
// *deprecated, and returns any other error unchanged.
func skipDeprecated(err error, deprecated *error) error {
	if err != nil && errors.Is(err, registry.ErrDeprecatedComponent) {
		if *deprecated == nil {
			*deprecated = err
		}
		return nil
	}
	return err
}
